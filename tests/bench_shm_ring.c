// Benchmarks for the shm_ring SPMC broadcast buffer.
//
//   throughput : single producer, no consumers — raw acquire/write/publish cost
//                across a payload-size sweep (isolates per-frame overhead vs
//                memcpy-bound regime).
//   latency    : producer + one busy-polling consumer — one-way publish->observe
//                latency (CLOCK_MONOTONIC is system-wide, so this is valid
//                across processes on the same host).
//   contended  : producer flooding + N consumers retaining/releasing — sustained
//                throughput under real contention.
//
//   make bench_shm_ring
//   ./bin/bench_shm_ring [throughput|latency|contended|all] [consumers]

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
#include <time.h>
#include <unistd.h>

#define BENCH_NAME "/shm_ring_bench"
#define MAX_CONSUMERS 64
#define LAT_SAMPLES 200000
#define LAT_WARMUP 2000
#define LAT_PACE_NS 5000 // ~200k frames/s so the consumer catches each frame

typedef struct {
  _Atomic int done;
  _Atomic uint64_t reads[MAX_CONSUMERS];
  _Atomic uint64_t lat[8]; // min p50 p90 p99 p999 max mean count
} bench_shared_t;

static uint64_t now_ns(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static int cmp_u64(const void *a, const void *b) {
  uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
  return (x > y) - (x < y);
}

static bench_shared_t *shared_new(void) {
  bench_shared_t *s = mmap(NULL, sizeof(*s), PROT_READ | PROT_WRITE,
                           MAP_SHARED | MAP_ANONYMOUS, -1, 0);
  if (s == MAP_FAILED) {
    perror("mmap shared");
    exit(2);
  }
  memset(s, 0, sizeof(*s));
  return s;
}

static void run_throughput(void) {
  const size_t sizes[] = {64, 256, 1024, 4096, 16384, 65536};
  printf("== throughput (single producer, no consumers) ==\n");
  printf("%10s %12s %12s %10s %10s\n", "payload", "frames", "Mframes/s",
         "GB/s", "ns/frame");
  for (size_t si = 0; si < sizeof(sizes) / sizeof(sizes[0]); si++) {
    size_t payload = sizes[si];
    uint64_t iters = 4000000000ULL / payload; // ~4 GB moved per size
    if (iters < 100000)
      iters = 100000;
    if (iters > 20000000)
      iters = 20000000;

    shm_unlink(BENCH_NAME);
    shm_ring_t *ring = shm_ring_create(BENCH_NAME, payload, 4);
    if (!ring) {
      perror("create");
      exit(2);
    }
    void *src = malloc(payload);
    memset(src, 0xab, payload);

    uint64_t t0 = now_ns();
    for (uint64_t i = 0; i < iters; i++) {
      int32_t idx = shm_ring_acquire_slot(ring); // never starves: no consumers
      memcpy(shm_ring_slot_data(ring, idx), src, payload);
      shm_ring_publish_slot(ring, idx);
    }
    uint64_t dt = now_ns() - t0;

    double secs = dt / 1e9;
    double fps = iters / secs;
    double gbps = (double)iters * payload / secs / 1e9;
    printf("%10zu %12llu %12.2f %10.2f %10.2f\n", payload,
           (unsigned long long)iters, fps / 1e6, gbps, (double)dt / iters);

    free(src);
    shm_ring_destroy(ring);
  }
}

typedef struct {
  uint64_t seq;
  uint64_t t_publish;
} lat_frame_t;

static void run_latency(void) {
  printf("\n== latency (one-way publish->observe, busy-poll consumer) ==\n");
  size_t payload = sizeof(lat_frame_t);
  shm_unlink(BENCH_NAME);
  shm_ring_t *ring = shm_ring_create(BENCH_NAME, payload, 4);
  if (!ring) {
    perror("create");
    exit(2);
  }
  bench_shared_t *sh = shared_new();

  pid_t pid = fork();
  if (pid == 0) {
    shm_ring_t *r = shm_ring_attach(BENCH_NAME);
    if (!r)
      _exit(2);
    uint64_t *samples = malloc(LAT_SAMPLES * sizeof(uint64_t));
    uint64_t last_seq = 0;
    size_t n = 0, seen = 0;
    while (n < LAT_SAMPLES && !atomic_load_explicit(&sh->done,
                                                    memory_order_acquire)) {
      int32_t idx = shm_ring_retain_latest(r);
      if (idx < 0)
        continue;
      lat_frame_t *f = (lat_frame_t *)shm_ring_slot_data(r, idx);
      uint64_t seq = f->seq, tp = f->t_publish;
      shm_ring_release_slot(r, idx);
      if (seq == last_seq)
        continue;
      last_seq = seq;
      if (seen++ < LAT_WARMUP)
        continue;
      samples[n++] = now_ns() - tp;
    }
    qsort(samples, n, sizeof(uint64_t), cmp_u64);
    uint64_t sum = 0;
    for (size_t i = 0; i < n; i++)
      sum += samples[i];
    if (n == 0)
      n = 1, samples[0] = 0;
    atomic_store_explicit(&sh->lat[0], samples[0], memory_order_relaxed);
    atomic_store_explicit(&sh->lat[1], samples[n / 2], memory_order_relaxed);
    atomic_store_explicit(&sh->lat[2], samples[(size_t)(n * 0.90)],
                          memory_order_relaxed);
    atomic_store_explicit(&sh->lat[3], samples[(size_t)(n * 0.99)],
                          memory_order_relaxed);
    atomic_store_explicit(&sh->lat[4], samples[(size_t)(n * 0.999)],
                          memory_order_relaxed);
    atomic_store_explicit(&sh->lat[5], samples[n - 1], memory_order_relaxed);
    atomic_store_explicit(&sh->lat[6], sum / n, memory_order_relaxed);
    atomic_store_explicit(&sh->lat[7], n, memory_order_relaxed);
    free(samples);
    shm_ring_destroy(r);
    _exit(0);
  }

  // Producer: paced so each frame is fresh when the consumer polls it.
  uint64_t published = 0;
  uint64_t next = now_ns();
  while (atomic_load_explicit(&sh->lat[7], memory_order_relaxed) == 0 &&
         published < (uint64_t)(LAT_SAMPLES + LAT_WARMUP) * 4) {
    int32_t idx = shm_ring_acquire_slot(ring);
    if (idx < 0)
      continue;
    lat_frame_t *f = (lat_frame_t *)shm_ring_slot_data(ring, idx);
    f->seq = ++published;
    f->t_publish = now_ns();
    shm_ring_publish_slot(ring, idx);
    next += LAT_PACE_NS;
    while (now_ns() < next)
      ;
  }
  atomic_store_explicit(&sh->done, 1, memory_order_release);
  waitpid(pid, NULL, 0);

  printf("  samples %llu (of %llu published)\n",
         (unsigned long long)atomic_load_explicit(&sh->lat[7],
                                                  memory_order_relaxed),
         (unsigned long long)published);
  const char *lbl[] = {"min", "p50", "p90", "p99", "p99.9", "max", "mean"};
  for (int i = 0; i < 7; i++)
    printf("  %-6s %8llu ns\n", lbl[i],
           (unsigned long long)atomic_load_explicit(&sh->lat[i],
                                                    memory_order_relaxed));
  shm_ring_destroy(ring);
  munmap(sh, sizeof(*sh));
}

static void run_contended(int consumers) {
  printf("\n== contended (1 producer flooding, %d consumers) ==\n", consumers);
  size_t payload = 4096;
  uint64_t iters = 10000000;
  shm_unlink(BENCH_NAME);
  shm_ring_t *ring = shm_ring_create(BENCH_NAME, payload, (uint32_t)consumers + 2);
  if (!ring) {
    perror("create");
    exit(2);
  }
  bench_shared_t *sh = shared_new();

  pid_t pids[MAX_CONSUMERS];
  for (int c = 0; c < consumers; c++) {
    pid_t pid = fork();
    if (pid == 0) {
      shm_ring_t *r = shm_ring_attach(BENCH_NAME);
      if (!r)
        _exit(2);
      uint64_t cnt = 0;
      while (!atomic_load_explicit(&sh->done, memory_order_acquire)) {
        int32_t idx = shm_ring_retain_latest(r);
        if (idx < 0)
          continue;
        shm_ring_release_slot(r, idx);
        cnt++;
      }
      atomic_store_explicit(&sh->reads[c], cnt, memory_order_relaxed);
      shm_ring_destroy(r);
      _exit(0);
    }
    pids[c] = pid;
  }

  void *src = malloc(payload);
  memset(src, 0xcd, payload);
  uint64_t t0 = now_ns();
  for (uint64_t i = 0; i < iters; i++) {
    int32_t idx;
    while ((idx = shm_ring_acquire_slot(ring)) < 0)
      ;
    memcpy(shm_ring_slot_data(ring, idx), src, payload);
    shm_ring_publish_slot(ring, idx);
  }
  uint64_t dt = now_ns() - t0;
  atomic_store_explicit(&sh->done, 1, memory_order_release);
  for (int c = 0; c < consumers; c++)
    waitpid(pids[c], NULL, 0);

  double secs = dt / 1e9;
  uint64_t total_reads = 0;
  for (int c = 0; c < consumers; c++)
    total_reads += atomic_load_explicit(&sh->reads[c], memory_order_relaxed);
  printf("  producer: %.2f Mframes/s (%.2f GB/s)\n", iters / secs / 1e6,
         (double)iters * payload / secs / 1e9);
  printf("  consumers: %.2f Mreads/s aggregate (%llu total)\n",
         total_reads / secs / 1e6, (unsigned long long)total_reads);

  free(src);
  shm_ring_destroy(ring);
  munmap(sh, sizeof(*sh));
}

int main(int argc, char **argv) {
  const char *mode = (argc > 1) ? argv[1] : "all";
  int consumers = (argc > 2) ? atoi(argv[2]) : 4;
  if (consumers < 1 || consumers > MAX_CONSUMERS)
    consumers = 4;

  int all = strcmp(mode, "all") == 0;
  if (all || strcmp(mode, "throughput") == 0)
    run_throughput();
  if (all || strcmp(mode, "latency") == 0)
    run_latency();
  if (all || strcmp(mode, "contended") == 0)
    run_contended(consumers);
  return 0;
}
