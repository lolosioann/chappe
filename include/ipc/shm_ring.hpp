#pragma once

extern "C" {
#include "shm_ring.h"
}

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>

namespace ipc {

// Non-owning view of a retained slot. Guarantees release() is called exactly
// once, even on exceptions unwinding through a handler. Move-only: a slot's
// "held" status must have exactly one owner, same reasoning as
// SubscriptionGuard in broker.hpp.
class ShmSlotView {
public:
  ShmSlotView() noexcept : ring_(nullptr), idx_(-1) {}

  ShmSlotView(shm_ring_t *ring, int32_t idx) noexcept : ring_(ring), idx_(idx) {}

  ~ShmSlotView() { release(); }

  ShmSlotView(const ShmSlotView &) = delete;
  ShmSlotView &operator=(const ShmSlotView &) = delete;

  ShmSlotView(ShmSlotView &&other) noexcept : ring_(other.ring_), idx_(other.idx_) {
    other.ring_ = nullptr;
    other.idx_ = -1;
  }

  ShmSlotView &operator=(ShmSlotView &&other) noexcept {
    if (this != &other) {
      release();
      ring_ = other.ring_;
      idx_ = other.idx_;
      other.ring_ = nullptr;
      other.idx_ = -1;
    }
    return *this;
  }

  bool valid() const noexcept { return ring_ != nullptr && idx_ >= 0; }
  explicit operator bool() const noexcept { return valid(); }

  void *data() const noexcept {
    return valid() ? shm_ring_slot_data(ring_, idx_) : nullptr;
  }

  size_t size() const noexcept { return valid() ? shm_ring_slot_size(ring_) : 0; }

  int32_t index() const noexcept { return idx_; }

private:
  void release() noexcept {
    if (valid()) {
      shm_ring_release_slot(ring_, idx_);
      ring_ = nullptr;
      idx_ = -1;
    }
  }

  shm_ring_t *ring_;
  int32_t idx_;
};

// Owning wrapper around shm_ring_t*. One instance per process per ring —
// producer constructs via create(), consumer via attach().
class SharedMemoryRing {
public:
  static SharedMemoryRing create(const std::string &name, size_t slot_size,
                                 uint32_t num_slots) {
    shm_ring_t *r = shm_ring_create(name.c_str(), slot_size, num_slots);
    if (!r)
      throw std::runtime_error("shm_ring_create failed for '" + name + "'");
    return SharedMemoryRing(r);
  }

  static SharedMemoryRing attach(const std::string &name) {
    shm_ring_t *r = shm_ring_attach(name.c_str());
    if (!r)
      throw std::runtime_error("shm_ring_attach failed for '" + name + "'");
    return SharedMemoryRing(r);
  }

  ~SharedMemoryRing() {
    if (ring_)
      shm_ring_destroy(ring_);
  }

  SharedMemoryRing(const SharedMemoryRing &) = delete;
  SharedMemoryRing &operator=(const SharedMemoryRing &) = delete;

  SharedMemoryRing(SharedMemoryRing &&other) noexcept : ring_(other.ring_) {
    other.ring_ = nullptr;
  }
  SharedMemoryRing &operator=(SharedMemoryRing &&other) noexcept {
    if (this != &other) {
      if (ring_)
        shm_ring_destroy(ring_);
      ring_ = other.ring_;
      other.ring_ = nullptr;
    }
    return *this;
  }

  // Producer side. A write-slot is not refcount-retained — the C layer's
  // WRITING state protects it from reclaim until publish(), so this hands back
  // a raw index + pointer rather than a ShmSlotView. Returns valid=false when
  // every slot is genuinely held by a consumer (see acquire_slot's policy).
  // NOTE: if the caller never publishes (e.g. throws mid-write), the slot stays
  // WRITING and is permanently skipped — the C layer has no abandon path.
  struct WriteHandle {
    int32_t idx;
    void *data;
    size_t size;
    bool valid;
  };

  WriteHandle acquire_write() noexcept {
    int32_t idx = shm_ring_acquire_slot(ring_);
    if (idx < 0)
      return {idx, nullptr, 0, false};
    return {idx, shm_ring_slot_data(ring_, idx), shm_ring_slot_size(ring_), true};
  }

  void publish(int32_t idx) noexcept { shm_ring_publish_slot(ring_, idx); }

  // Consumer side. Refcount-guarded — safe to hold across the handler call.
  ShmSlotView retain_latest() noexcept {
    int32_t idx = shm_ring_retain_latest(ring_);
    if (idx < 0)
      return ShmSlotView{};
    return ShmSlotView(ring_, idx);
  }

  size_t slot_size() const noexcept { return shm_ring_slot_size(ring_); }

private:
  explicit SharedMemoryRing(shm_ring_t *r) : ring_(r) {}
  shm_ring_t *ring_;
};

} // namespace ipc
