#ifndef SHM_RING_H
#define SHM_RING_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Single-producer / multi-consumer "latest frame" broadcast ring in POSIX
// shared memory. The producer publishes frames into recyclable slots; consumers
// grab the newest published frame and hold it (refcounted) for zero-copy reads.
// Not a FIFO queue — consumers only ever see the latest completed frame.

typedef enum {
  SLOT_FREE = 0,
  SLOT_WRITING = 1,
  SLOT_READY = 2
} shm_slot_state_t;

typedef struct shm_ring shm_ring_t;

// --- producer side ---
// num_slots must be >= 2 (one slot always holds the published frame).
shm_ring_t *shm_ring_create(const char *name, size_t slot_size,
                            uint32_t num_slots);
// Returns a slot index to write into, or -1 if every slot is held by a
// consumer (starved). Write payload via shm_ring_slot_data, then publish.
int32_t shm_ring_acquire_slot(shm_ring_t *ring);
void shm_ring_publish_slot(shm_ring_t *ring, int32_t idx);

// --- consumer side ---
// Geometry (slot_size, num_slots) is read from the shared segment.
shm_ring_t *shm_ring_attach(const char *name);
// Retains and returns the newest published frame's slot index, or -1 if none
// is available. The returned slot stays valid until shm_ring_release_slot.
int32_t shm_ring_retain_latest(shm_ring_t *ring);
void shm_ring_release_slot(shm_ring_t *ring, int32_t idx);

// --- shared ---
void *shm_ring_slot_data(shm_ring_t *ring, int32_t idx);
size_t shm_ring_slot_size(shm_ring_t *ring);
void shm_ring_destroy(shm_ring_t *ring);

#ifdef __cplusplus
}
#endif
#endif
