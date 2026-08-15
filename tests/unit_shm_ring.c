/*
 * Plumbing self-check: geometry round-trip + one publish/retain cycle
 * through a second mapping.  Does not exercise the producer/consumer
 * race (that needs real concurrency); it verifies layout, attach,
 * publish, retain, and payload visibility.
 *
 * Build: cc tests/unit_shm_ring.c src/shm_ring.c -Iinclude -o t -lrt && ./t
 */

#define _POSIX_C_SOURCE 200809L
#include "shm_ring.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>

int main(void)
{
	const char *name = "/shm_ring_selftest";
	shm_ring_t *prod;
	shm_ring_t *cons;
	s32 w;
	s32 r;

	/* Clear any stale segment from a crashed run. */
	shm_unlink(name);

	prod = shm_ring_create(name, 16, 4);
	assert(prod);

	w = shm_ring_acquire_slot(prod);
	assert(w >= 0);
	memcpy(shm_ring_slot_data(prod, w), "hello-frame-01", 15);
	shm_ring_publish_slot(prod, w);

	cons = shm_ring_attach(name);
	assert(cons);
	assert(shm_ring_slot_size(cons) == 16);

	r = shm_ring_retain_latest(cons);
	assert(r >= 0);
	assert(memcmp(shm_ring_slot_data(cons, r), "hello-frame-01", 15) == 0);
	shm_ring_release_slot(cons, r);

	shm_ring_destroy(cons);
	shm_ring_destroy(prod);
	printf("shm_ring self-check OK\n");
	return 0;
}
