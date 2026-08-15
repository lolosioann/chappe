/*
 * Lock Free Ring Buffer
 * 2026 Lolos Ioannis <lolosioann@gmail.com>
 *
 * chappe/src/shm_ring.c
 *
 * Single-producer / multi-consumer ring in POSIX shared memory.
 * It is used in overwrite mode. The producers overwrite the
 * oldest data if no free slots are left to write to.
 * Consumers retain the newest published data using reference
 * counting for zero-copy reads.
 * Non-FIFO.
 */

/*
 * Pull in POSIX.1-2008 for shm_open() and shm_unlink().
 * Must be first.
 */
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
#define SHM_RING_MAGIC 0x53484d52u /* "SHMR" */
#define SHM_CACHELINE 64u
#define SHM_LAST_NONE UINT32_MAX
#define SHM_RETAIN_TRIES 4

/* Round x up to the next multiple of a. */
#define SHM_ALIGN_UP(x, a) \
	(((size_t)(x) + ((size_t)(a) - 1)) & ~((size_t)(a) - 1))

/*
 * Shared memory layout:
 *   [struct shm_ctrl] [pad to cache line] [slot 0] [slot 1] ... [slot N]
 *
 * Control block at offset 0 of the mapping. Lives in shared memory so a
 * consumer that only knows the name can recover the geometry, and so
 * the producer's published-frame cursor is visible across processes.
 */
struct shm_ctrl {
	_Atomic u32 magic; /* written last on create; gates attach */
	u32 num_slots;
	u64 slot_size;
	_Atomic u32 last_ready; /* newest published slot, or NONE */
	u32 _pad;
};

/* Per-slot header, one per slot. */
struct shm_slot_hdr {
	_Atomic u32 state;
	_Atomic u32 refcount;
};

/*
 * Slots begin one cache line past the control block so the hot
 * last_ready field never shares a line with slot 0's header.
 */
static const size_t SLOTS_OFFSET =
    SHM_ALIGN_UP(sizeof(struct shm_ctrl), SHM_CACHELINE);

/*
 * Process-local handle.  Only ctrl and base live in shared memory.
 */
struct shm_ring {
	struct shm_ctrl *ctrl;
	void *base;
	size_t total_size;
	size_t stride; /* bytes between consecutive slot headers */
	u32 num_slots;
	size_t slot_size;
	u32 write_cursor; /* producer-local scan position */
	int fd;
	int is_owner;
	char name[SHM_RING_MAX_NAME];
};

/* Pad one slot per cache line */
static size_t slot_stride(size_t slot_size)
{
	return SHM_ALIGN_UP(sizeof(struct shm_slot_hdr) + slot_size,
			    SHM_CACHELINE);
}

static struct shm_slot_hdr *slot_header(shm_ring_t *ring, u32 idx)
{
	return (struct shm_slot_hdr *)((char *)ring->base + SLOTS_OFFSET +
				       (size_t)idx * ring->stride);
}

static void *slot_payload(shm_ring_t *ring, u32 idx)
{
	return (char *)slot_header(ring, idx) + sizeof(struct shm_slot_hdr);
}

shm_ring_t *shm_ring_create(const char *name, size_t slot_size, u32 num_slots)
{
	shm_ring_t *ring;
	u32 i;

	/* Need >= 2: one slot always holds the published frame. */
	if (num_slots < 2)
		return NULL;

	ring = calloc(1, sizeof(*ring));
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

	ring->base = mmap(NULL, ring->total_size, PROT_READ | PROT_WRITE,
			  MAP_SHARED, ring->fd, 0);
	if (ring->base == MAP_FAILED) {
		close(ring->fd);
		shm_unlink(name);
		free(ring);
		return NULL;
	}

	ring->ctrl = (struct shm_ctrl *)ring->base;
	ring->ctrl->num_slots = num_slots;
	ring->ctrl->slot_size = slot_size;
	atomic_init(&ring->ctrl->last_ready, SHM_LAST_NONE);
	for (i = 0; i < num_slots; i++) {
		struct shm_slot_hdr *h = slot_header(ring, i);

		atomic_init(&h->state, SLOT_FREE);
		atomic_init(&h->refcount, 0);
	}

	/*
	 * Publish magic last (release): a consumer that reads MAGIC is
	 * guaranteed to see the fully initialized control block and
	 * slots.
	 */
	atomic_store_explicit(&ring->ctrl->magic, SHM_RING_MAGIC,
			      memory_order_release);

	return ring;
}

shm_ring_t *shm_ring_attach(const char *name)
{
	shm_ring_t *ring;
	struct stat st;

	ring = calloc(1, sizeof(*ring));
	if (!ring)
		return NULL;

	strncpy(ring->name, name, SHM_RING_MAX_NAME - 1);
	ring->is_owner = 0;

	ring->fd = shm_open(name, O_RDWR, 0);
	if (ring->fd < 0) {
		free(ring);
		return NULL;
	}

	if (fstat(ring->fd, &st) != 0) {
		close(ring->fd);
		free(ring);
		return NULL;
	}
	ring->total_size = (size_t)st.st_size;

	ring->base = mmap(NULL, ring->total_size, PROT_READ | PROT_WRITE,
			  MAP_SHARED, ring->fd, 0);
	if (ring->base == MAP_FAILED) {
		close(ring->fd);
		free(ring);
		return NULL;
	}

	ring->ctrl = (struct shm_ctrl *)ring->base;
	if (atomic_load_explicit(&ring->ctrl->magic, memory_order_acquire) !=
	    SHM_RING_MAGIC) {
		munmap(ring->base, ring->total_size);
		close(ring->fd);
		free(ring);
		/* not created yet, or wrong/corrupt segment */
		return NULL;
	}

	ring->num_slots = ring->ctrl->num_slots;
	ring->slot_size = (size_t)ring->ctrl->slot_size;
	ring->stride = slot_stride(ring->slot_size);

	return ring;
}

s32 shm_ring_acquire_slot(shm_ring_t *ring)
{
	u32 published;
	u32 attempt;
	u32 idx;
	u32 prev;
	struct shm_slot_hdr *h;

	published =
	    atomic_load_explicit(&ring->ctrl->last_ready, memory_order_relaxed);

	for (attempt = 1; attempt <= ring->num_slots; attempt++) {
		idx = (ring->write_cursor + attempt) % ring->num_slots;

		/* Never clobber the frame consumers are currently offered. */
		if (idx == published)
			continue;

		h = slot_header(ring, idx);

		/* Cheap pre-filter: skip slots obviously held by consumers. */
		if (atomic_load_explicit(&h->refcount, memory_order_relaxed) !=
		    0)
			continue;

		prev = atomic_load_explicit(&h->state, memory_order_relaxed);

		/* Defensive: skip our own in-flight unpublished slot. */
		if (prev == SLOT_WRITING)
			continue;

		/*
		 * Claim, then re-check refcount.  The seq_cst store/load
		 * pair here mirrors the consumer's
		 * fetch_add(refcount)+load(state) in
		 * shm_ring_retain_latest: seq_cst forbids store-load
		 * reordering across locations, so one side always
		 * observes the other.  If a consumer slipped in after
		 * the pre-filter, we back off rather than overwrite a
		 * slot it's about to read.
		 */
		atomic_store_explicit(&h->state, SLOT_WRITING,
				      memory_order_seq_cst);
		if (atomic_load_explicit(&h->refcount, memory_order_seq_cst) !=
		    0) {
			/*
			 * A slot with refcount>0 was published
			 * (last_ready pointed here), so its prior state
			 * is READY and its payload is intact -- restore it.
			 */
			atomic_store_explicit(&h->state, prev,
					      memory_order_release);
			continue;
		}

		ring->write_cursor = idx;
		return (s32)idx;
	}

	/* every reusable slot held by a consumer - ring starved */
	return -1;
}

void shm_ring_publish_slot(shm_ring_t *ring, s32 idx)
{
	struct shm_slot_hdr *h = slot_header(ring, (u32)idx);

	/*
	 * Release: payload writes before this call are visible to any
	 * consumer that observes SLOT_READY (via the seq_cst state load
	 * in retain_latest).
	 */
	atomic_store_explicit(&h->state, SLOT_READY, memory_order_release);
	atomic_store_explicit(&ring->ctrl->last_ready, (u32)idx,
			      memory_order_release);
}

void shm_ring_abandon_slot(shm_ring_t *ring, s32 idx)
{
	struct shm_slot_hdr *h = slot_header(ring, (u32)idx);

	/*
	 * A WRITING slot the producer decided not to publish (e.g. the
	 * writer threw mid-fill).  It was never published, so
	 * last_ready never pointed at it and no consumer can hold it --
	 * just return it to the free pool so it isn't skipped forever.
	 */
	atomic_store_explicit(&h->state, SLOT_FREE, memory_order_release);
}

s32 shm_ring_retain_latest(shm_ring_t *ring)
{
	int tries;
	u32 idx;
	struct shm_slot_hdr *h;

	for (tries = 0; tries < SHM_RETAIN_TRIES; tries++) {
		idx = atomic_load_explicit(&ring->ctrl->last_ready,
					   memory_order_acquire);

		if (idx == SHM_LAST_NONE || idx >= ring->num_slots)
			return -1;

		h = slot_header(ring, idx);

		/*
		 * Stake our claim first, then verify.  See the handshake
		 * note in shm_ring_acquire_slot -- the seq_cst pairing is
		 * what makes this safe.
		 */
		atomic_fetch_add_explicit(&h->refcount, 1,
					  memory_order_seq_cst);
		if (atomic_load_explicit(&h->state, memory_order_seq_cst) ==
		    SLOT_READY)
			return (s32)idx;

		/*
		 * Producer reclaimed or is rewriting this slot; give it
		 * back and retry against a freshly-read last_ready.
		 */
		atomic_fetch_sub_explicit(&h->refcount, 1,
					  memory_order_acq_rel);
	}

	return -1;
}

void shm_ring_release_slot(shm_ring_t *ring, s32 idx)
{
	struct shm_slot_hdr *h = slot_header(ring, (u32)idx);

	atomic_fetch_sub_explicit(&h->refcount, 1, memory_order_acq_rel);
}

void *shm_ring_slot_data(shm_ring_t *ring, s32 idx)
{
	return slot_payload(ring, (u32)idx);
}

size_t shm_ring_slot_size(shm_ring_t *ring)
{
	return ring->slot_size;
}

void shm_ring_destroy(shm_ring_t *ring)
{
	if (!ring)
		return;

	munmap(ring->base, ring->total_size);
	close(ring->fd);
	if (ring->is_owner)
		shm_unlink(ring->name);
	free(ring);
}
