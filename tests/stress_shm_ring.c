// Fork-based concurrency stress test for the shm_ring seq_cst handshake.
//
// One producer process publishes frames as fast as it can, each frame carrying
// an incrementing sequence number and an FNV-1a checksum over its payload.
// Several consumer processes retain the latest frame, recompute the checksum
// over exactly the bytes they read, compare, and release — in a tight loop.
//
// The invariant: a retained frame is immutable until released. If the handshake
// is broken (e.g. acquire/release instead of seq_cst), the producer will reuse
// a slot a consumer is mid-read on, the payload will tear, and the recomputed
// checksum will not match the stored one. Any mismatch is a real bug.
//
//   make stress_shm_ring
//   ./bin/stress_shm_ring [iterations] [consumers] [payload_bytes] [slots]

#define _POSIX_C_SOURCE 200809L
#include "shm_ring.h"
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

#define STRESS_NAME "/shm_ring_stress"
#define MAX_CONSUMERS 64

typedef struct {
  uint64_t seq;
  uint64_t checksum;
  uint8_t fill[]; // slot_size - 16 bytes
} frame_t;

// Anonymous MAP_SHARED coordination block, inherited across fork().
typedef struct {
  _Atomic int done;      // producer finished
  _Atomic int error;     // a torn read was detected
  _Atomic uint64_t err_seq;
  _Atomic uint64_t err_stored;
  _Atomic uint64_t err_computed;
  _Atomic uint64_t reads[MAX_CONSUMERS];
  _Atomic uint64_t misses[MAX_CONSUMERS];
  _Atomic uint64_t maxseq[MAX_CONSUMERS];
} coord_t;

static uint64_t fnv1a(const void *p, size_t n) {
  const uint8_t *b = (const uint8_t *)p;
  uint64_t h = 1469598103934665603ULL;
  for (size_t i = 0; i < n; i++) {
    h ^= b[i];
    h *= 1099511628211ULL;
  }
  return h;
}

static void producer(shm_ring_t *ring, coord_t *co, uint64_t iterations,
                     size_t fill_len) {
  for (uint64_t seq = 1; seq <= iterations; seq++) {
    if (atomic_load_explicit(&co->error, memory_order_acquire))
      break;

    int32_t idx;
    while ((idx = shm_ring_acquire_slot(ring)) < 0) {
      // Every reusable slot is held by a consumer; spin until one frees.
      if (atomic_load_explicit(&co->error, memory_order_acquire))
        return;
    }

    frame_t *f = (frame_t *)shm_ring_slot_data(ring, idx);
    for (size_t i = 0; i < fill_len; i++)
      f->fill[i] = (uint8_t)(seq + i);
    f->seq = seq;
    f->checksum = fnv1a(f->fill, fill_len);
    shm_ring_publish_slot(ring, idx);
  }
  atomic_store_explicit(&co->done, 1, memory_order_release);
}

static void consumer(shm_ring_t *ring, coord_t *co, int id, size_t fill_len) {
  while (!atomic_load_explicit(&co->done, memory_order_acquire) &&
         !atomic_load_explicit(&co->error, memory_order_acquire)) {
    int32_t idx = shm_ring_retain_latest(ring);
    if (idx < 0) {
      atomic_fetch_add_explicit(&co->misses[id], 1, memory_order_relaxed);
      continue;
    }

    frame_t *f = (frame_t *)shm_ring_slot_data(ring, idx);
    uint64_t seq = f->seq;
    uint64_t stored = f->checksum;
    uint64_t computed = fnv1a(f->fill, fill_len);
    shm_ring_release_slot(ring, idx);

    if (computed != stored) {
      atomic_store_explicit(&co->err_seq, seq, memory_order_relaxed);
      atomic_store_explicit(&co->err_stored, stored, memory_order_relaxed);
      atomic_store_explicit(&co->err_computed, computed, memory_order_relaxed);
      atomic_store_explicit(&co->error, 1, memory_order_release);
      return;
    }

    atomic_fetch_add_explicit(&co->reads[id], 1, memory_order_relaxed);
    if (seq > atomic_load_explicit(&co->maxseq[id], memory_order_relaxed))
      atomic_store_explicit(&co->maxseq[id], seq, memory_order_relaxed);
  }
}

int main(int argc, char **argv) {
  uint64_t iterations = (argc > 1) ? strtoull(argv[1], NULL, 10) : 5000000;
  int consumers = (argc > 2) ? atoi(argv[2]) : 4;
  size_t payload = (argc > 3) ? (size_t)strtoull(argv[3], NULL, 10) : 8192;
  uint32_t slots = (argc > 4) ? (uint32_t)atoi(argv[4]) : (uint32_t)consumers + 2;

  if (consumers < 1 || consumers > MAX_CONSUMERS) {
    fprintf(stderr, "consumers must be 1..%d\n", MAX_CONSUMERS);
    return 2;
  }
  if (payload < sizeof(frame_t) + 16) {
    fprintf(stderr, "payload must be >= %zu\n", sizeof(frame_t) + 16);
    return 2;
  }
  size_t fill_len = payload - sizeof(frame_t);

  printf("stress: %llu frames, %d consumers, %zuB payload, %u slots\n",
         (unsigned long long)iterations, consumers, payload, slots);

  coord_t *co = mmap(NULL, sizeof(coord_t), PROT_READ | PROT_WRITE,
                     MAP_SHARED | MAP_ANONYMOUS, -1, 0);
  if (co == MAP_FAILED) {
    perror("mmap coord");
    return 2;
  }
  memset(co, 0, sizeof(*co));

  shm_unlink(STRESS_NAME); // clear any stale segment from a crashed run
  shm_ring_t *ring = shm_ring_create(STRESS_NAME, payload, slots);
  if (!ring) {
    perror("shm_ring_create");
    return 2;
  }

  pid_t pids[MAX_CONSUMERS];
  for (int c = 0; c < consumers; c++) {
    pid_t pid = fork();
    if (pid < 0) {
      perror("fork");
      atomic_store_explicit(&co->error, 1, memory_order_release);
      break;
    }
    if (pid == 0) {
      shm_ring_t *r = shm_ring_attach(STRESS_NAME);
      if (!r) {
        atomic_store_explicit(&co->error, 1, memory_order_release);
        _exit(2);
      }
      consumer(r, co, c, fill_len);
      shm_ring_destroy(r);
      _exit(atomic_load_explicit(&co->error, memory_order_acquire) ? 1 : 0);
    }
    pids[c] = pid;
  }

  producer(ring, co, iterations, fill_len);

  for (int c = 0; c < consumers; c++)
    waitpid(pids[c], NULL, 0);

  shm_ring_destroy(ring);

  uint64_t total_reads = 0, total_misses = 0;
  for (int c = 0; c < consumers; c++) {
    uint64_t r = atomic_load_explicit(&co->reads[c], memory_order_relaxed);
    uint64_t m = atomic_load_explicit(&co->misses[c], memory_order_relaxed);
    uint64_t s = atomic_load_explicit(&co->maxseq[c], memory_order_relaxed);
    total_reads += r;
    total_misses += m;
    printf("  consumer %d: %llu reads, %llu misses, max seq %llu\n", c,
           (unsigned long long)r, (unsigned long long)m,
           (unsigned long long)s);
  }
  printf("total: %llu verified reads, %llu misses\n",
         (unsigned long long)total_reads, (unsigned long long)total_misses);

  int err = atomic_load_explicit(&co->error, memory_order_acquire);
  if (err) {
    printf("FAIL: torn read at seq %llu (stored %016llx computed %016llx)\n",
           (unsigned long long)atomic_load_explicit(&co->err_seq,
                                                    memory_order_relaxed),
           (unsigned long long)atomic_load_explicit(&co->err_stored,
                                                    memory_order_relaxed),
           (unsigned long long)atomic_load_explicit(&co->err_computed,
                                                    memory_order_relaxed));
  } else {
    printf("PASS: no torn reads\n");
  }

  munmap(co, sizeof(coord_t));
  return err ? 1 : 0;
}
