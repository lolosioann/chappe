/*
 * Lock Free Ring Buffer
 * 2026 Lolos Ioannis <lolosioann@gmail.com>
 *
 * chappe/include/shm_ring.h
 *
 * Single-producer / multi-consumer ring in POSIX shared memory.
 * It is used in overwrite mode. The producers overwrite the
 * oldest data if no free slots are left to write to.
 * Consumers retain the newest published data using reference
 * counting for zero-copy reads.
 * Non-FIFO.
 */

#ifndef SHM_RING_H
#define SHM_RING_H

#include <stddef.h>
#include <stdint.h>

typedef uint32_t u32;
typedef uint64_t u64;
typedef int32_t s32;

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
	SLOT_FREE = 0,
	SLOT_WRITING = 1,
	SLOT_READY = 2
} shm_slot_state_t;

typedef struct shm_ring shm_ring_t;

/* --- producer side --- */
/* num_slots must be >= 2 (one slot always holds the published frame). */
shm_ring_t *shm_ring_create(const char *name, size_t slot_size, u32 num_slots);
/*
 * Returns a slot index to write into, or -1 if every slot is held by a
 * consumer (starved). Write payload via shm_ring_slot_data, then publish.
 */
s32 shm_ring_acquire_slot(shm_ring_t *ring);
void shm_ring_publish_slot(shm_ring_t *ring, s32 idx);
/* Return an acquired-but-unpublished slot to the free pool (writer bailed out).
 */
void shm_ring_abandon_slot(shm_ring_t *ring, s32 idx);

/* --- consumer side --- */
/* Geometry (slot_size, num_slots) is read from the shared segment. */
shm_ring_t *shm_ring_attach(const char *name);
/*
 * Retains and returns the newest published frame's slot index, or -1 if none
 * is available. The returned slot stays valid until shm_ring_release_slot.
 */
s32 shm_ring_retain_latest(shm_ring_t *ring);
void shm_ring_release_slot(shm_ring_t *ring, s32 idx);

/* --- shared --- */
void *shm_ring_slot_data(shm_ring_t *ring, s32 idx);
size_t shm_ring_slot_size(shm_ring_t *ring);
void shm_ring_destroy(shm_ring_t *ring);

#ifdef __cplusplus
}
#endif
#endif
