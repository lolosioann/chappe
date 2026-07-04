#define _POSIX_C_SOURCE 200809L
#include "shm_ring.h"
#include <fcntl.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#define SHM_RING_MAX_NAME 64
#define SHM_RING_MAGIC 0x53484d52u // "SHMR"
#define SHM_CACHELINE 64u
#define SHM_LAST_NONE UINT32_MAX
#define SHM_RETAIN_TRIES 4

#define SHM_ALIGN_UP(x, a)                                                     \
  (((size_t)(x) + ((size_t)(a) - 1)) & ~((size_t)(a) - 1))

// Control block at offset 0 of the mapping. Lives in shared memory so a
// consumer that only knows the name can recover the geometry, and so the
// producer's published-frame cursor is visible across processes.
typedef struct {
  _Atomic uint32_t magic; // written last on create; gates attach
  uint32_t num_slots;
  uint64_t slot_size;
  _Atomic uint32_t last_ready; // index of newest published slot, or NONE
  uint32_t _pad;
} shm_ctrl_t;

// Per-slot header, in shared memory, one per slot.
typedef struct {
  _Atomic uint32_t state;
  _Atomic uint32_t refcount;
} shm_slot_header_t;

// Slots begin one cache line past the control block so the hot last_ready
// field never shares a line with slot 0's header.
static const size_t SLOTS_OFFSET =
    (sizeof(shm_ctrl_t) + (SHM_CACHELINE - 1)) & ~((size_t)SHM_CACHELINE - 1);

// Process-local handle. Only ctrl/base live in shared memory.
struct shm_ring {
  shm_ctrl_t *ctrl;
  void *base;
  size_t total_size;
  size_t stride; // bytes between consecutive slot headers
  uint32_t num_slots;
  size_t slot_size;
  uint32_t write_cursor; // producer-local scan position
  int fd;
  int is_owner;
  char name[SHM_RING_MAX_NAME];
};

static size_t slot_stride(size_t slot_size) {
  // ponytail: pad each slot to a cache line to avoid producer/consumer false
  // sharing on adjacent slot headers. Drop the align if memory is tight.
  return SHM_ALIGN_UP(sizeof(shm_slot_header_t) + slot_size, SHM_CACHELINE);
}

static shm_slot_header_t *slot_header(shm_ring_t *ring, uint32_t idx) {
  return (shm_slot_header_t *)((char *)ring->base + SLOTS_OFFSET +
                               (size_t)idx * ring->stride);
}

static void *slot_payload(shm_ring_t *ring, uint32_t idx) {
  return (char *)slot_header(ring, idx) + sizeof(shm_slot_header_t);
}

shm_ring_t *shm_ring_create(const char *name, size_t slot_size,
                            uint32_t num_slots) {
  if (num_slots < 2)
    return NULL; // need >=2: one slot always holds the published frame

  shm_ring_t *ring = calloc(1, sizeof(shm_ring_t));
  if (!ring)
    return NULL;

  strncpy(ring->name, name, SHM_RING_MAX_NAME - 1);
  ring->slot_size = slot_size;
  ring->num_slots = num_slots;
  ring->stride = slot_stride(slot_size);
  ring->is_owner = 1;
  ring->total_size = SLOTS_OFFSET + (size_t)num_slots * ring->stride;

  ring->fd = shm_open(name, O_CREAT | O_EXCL | O_RDWR, 0660);
  if (ring->fd < 0) {
    free(ring);
    return NULL;
  }

  if (ftruncate(ring->fd, (off_t)ring->total_size) != 0) {
    close(ring->fd);
    shm_unlink(name);
    free(ring);
    return NULL;
  }

  ring->base = mmap(NULL, ring->total_size, PROT_READ | PROT_WRITE, MAP_SHARED,
                    ring->fd, 0);
  if (ring->base == MAP_FAILED) {
    close(ring->fd);
    shm_unlink(name);
    free(ring);
    return NULL;
  }

  ring->ctrl = (shm_ctrl_t *)ring->base;
  ring->ctrl->num_slots = num_slots;
  ring->ctrl->slot_size = slot_size;
  atomic_init(&ring->ctrl->last_ready, SHM_LAST_NONE);
  for (uint32_t i = 0; i < num_slots; i++) {
    shm_slot_header_t *h = slot_header(ring, i);
    atomic_init(&h->state, SLOT_FREE);
    atomic_init(&h->refcount, 0);
  }
  // Publish magic last (release): a consumer that reads MAGIC is guaranteed to
  // see the fully initialized control block and slots.
  atomic_store_explicit(&ring->ctrl->magic, SHM_RING_MAGIC,
                        memory_order_release);

  return ring;
}

shm_ring_t *shm_ring_attach(const char *name) {
  shm_ring_t *ring = calloc(1, sizeof(shm_ring_t));
  if (!ring)
    return NULL;

  strncpy(ring->name, name, SHM_RING_MAX_NAME - 1);
  ring->is_owner = 0;

  ring->fd = shm_open(name, O_RDWR, 0);
  if (ring->fd < 0) {
    free(ring);
    return NULL;
  }

  struct stat st;
  if (fstat(ring->fd, &st) != 0) {
    close(ring->fd);
    free(ring);
    return NULL;
  }
  ring->total_size = (size_t)st.st_size;

  ring->base = mmap(NULL, ring->total_size, PROT_READ | PROT_WRITE, MAP_SHARED,
                    ring->fd, 0);
  if (ring->base == MAP_FAILED) {
    close(ring->fd);
    free(ring);
    return NULL;
  }

  ring->ctrl = (shm_ctrl_t *)ring->base;
  if (atomic_load_explicit(&ring->ctrl->magic, memory_order_acquire) !=
      SHM_RING_MAGIC) {
    munmap(ring->base, ring->total_size);
    close(ring->fd);
    free(ring);
    return NULL; // not created yet, or wrong/corrupt segment
  }

  ring->num_slots = ring->ctrl->num_slots;
  ring->slot_size = (size_t)ring->ctrl->slot_size;
  ring->stride = slot_stride(ring->slot_size);
  return ring;
}

int32_t shm_ring_acquire_slot(shm_ring_t *ring) {
  uint32_t published =
      atomic_load_explicit(&ring->ctrl->last_ready, memory_order_relaxed);

  for (uint32_t attempt = 1; attempt <= ring->num_slots; attempt++) {
    uint32_t idx = (ring->write_cursor + attempt) % ring->num_slots;
    if (idx == published)
      continue; // never clobber the frame consumers are currently offered

    shm_slot_header_t *h = slot_header(ring, idx);

    // Cheap pre-filter: skip slots obviously held by a consumer.
    if (atomic_load_explicit(&h->refcount, memory_order_relaxed) != 0)
      continue;

    uint32_t prev = atomic_load_explicit(&h->state, memory_order_relaxed);
    if (prev == SLOT_WRITING)
      continue; // an in-flight slot we haven't published (defensive)

    // Claim, then re-check refcount. The seq_cst store/load pair here mirrors
    // the consumer's fetch_add(refcount)+load(state) in shm_ring_retain_latest:
    // seq_cst forbids store-load reordering across locations, so one side
    // always observes the other. If a consumer slipped in after the pre-filter,
    // we back off rather than overwrite a slot it's about to read.
    atomic_store_explicit(&h->state, SLOT_WRITING, memory_order_seq_cst);
    if (atomic_load_explicit(&h->refcount, memory_order_seq_cst) != 0) {
      // A slot with refcount>0 was published (last_ready pointed here), so its
      // prior state is READY and its payload is intact — restore it.
      atomic_store_explicit(&h->state, prev, memory_order_release);
      continue;
    }

    ring->write_cursor = idx;
    return (int32_t)idx;
  }

  return -1; // every reusable slot is held by a consumer — genuinely starved
}

void shm_ring_publish_slot(shm_ring_t *ring, int32_t idx) {
  shm_slot_header_t *h = slot_header(ring, (uint32_t)idx);
  // release: payload writes before this call are visible to any consumer that
  // observes SLOT_READY (via the seq_cst state load in retain_latest).
  atomic_store_explicit(&h->state, SLOT_READY, memory_order_release);
  atomic_store_explicit(&ring->ctrl->last_ready, (uint32_t)idx,
                        memory_order_release);
}

int32_t shm_ring_retain_latest(shm_ring_t *ring) {
  for (int tries = 0; tries < SHM_RETAIN_TRIES; tries++) {
    uint32_t idx =
        atomic_load_explicit(&ring->ctrl->last_ready, memory_order_acquire);
    if (idx == SHM_LAST_NONE || idx >= ring->num_slots)
      return -1;

    shm_slot_header_t *h = slot_header(ring, idx);
    // Stake our claim first, then verify. See the handshake note in
    // shm_ring_acquire_slot — the seq_cst pairing is what makes this safe.
    atomic_fetch_add_explicit(&h->refcount, 1, memory_order_seq_cst);
    if (atomic_load_explicit(&h->state, memory_order_seq_cst) == SLOT_READY)
      return (int32_t)idx;

    // Producer reclaimed or is rewriting this slot; give it back and retry
    // against a freshly-read last_ready.
    atomic_fetch_sub_explicit(&h->refcount, 1, memory_order_acq_rel);
  }
  return -1;
}

void shm_ring_release_slot(shm_ring_t *ring, int32_t idx) {
  shm_slot_header_t *h = slot_header(ring, (uint32_t)idx);
  atomic_fetch_sub_explicit(&h->refcount, 1, memory_order_acq_rel);
}

void *shm_ring_slot_data(shm_ring_t *ring, int32_t idx) {
  return slot_payload(ring, (uint32_t)idx);
}

size_t shm_ring_slot_size(shm_ring_t *ring) { return ring->slot_size; }

void shm_ring_destroy(shm_ring_t *ring) {
  if (!ring)
    return;
  munmap(ring->base, ring->total_size);
  close(ring->fd);
  if (ring->is_owner)
    shm_unlink(ring->name);
  free(ring);
}

#ifdef SHM_RING_TEST
#include <assert.h>
#include <stdio.h>
// Plumbing self-check: geometry round-trip + one publish/retain cycle through a
// second mapping. Does not exercise the producer/consumer race (that needs real
// concurrency); it verifies layout, attach, publish, retain, and payload
// visibility. Build: cc -DSHM_RING_TEST src/shm_ring.c -o t -lrt && ./t
int main(void) {
  const char *name = "/shm_ring_selftest";
  shm_unlink(name); // clear any stale segment from a crashed run

  shm_ring_t *prod = shm_ring_create(name, 16, 4);
  assert(prod);

  int32_t w = shm_ring_acquire_slot(prod);
  assert(w >= 0);
  memcpy(shm_ring_slot_data(prod, w), "hello-frame-01", 15);
  shm_ring_publish_slot(prod, w);

  shm_ring_t *cons = shm_ring_attach(name);
  assert(cons);
  assert(shm_ring_slot_size(cons) == 16);

  int32_t r = shm_ring_retain_latest(cons);
  assert(r >= 0);
  assert(memcmp(shm_ring_slot_data(cons, r), "hello-frame-01", 15) == 0);
  shm_ring_release_slot(cons, r);

  shm_ring_destroy(cons);
  shm_ring_destroy(prod);
  printf("shm_ring self-check OK\n");
  return 0;
}
#endif
