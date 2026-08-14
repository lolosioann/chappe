// examples/frames.cpp
// The shared-memory frame transport: a camera publishes frames into a shm ring
// and a vision node processes them zero-copy. Only the small FrameHandle rides
// the broker; the pixels never do. It works identically across processes — the
// two nodes live in one process here just to keep the example runnable.
#include "server.hpp"
#include "node.hpp"
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <sys/mman.h>
#include <thread>

struct Camera : chappe::FrameHandle {};
MAKE_TOPIC(Camera, "cam/front");

int main() {
  shm_unlink("/chappe_cam_front"); // clear any stale segment from a prior run
  chappe::Server broker;

  chappe::Node camera("camera");
  chappe::Node vision("vision");
  camera.connect();
  vision.connect();

  // 640x480 grayscale frames, 4 slots. The producer owns the ring; the consumer
  // attaches to the same segment (named from the topic, so no shared config).
  const uint32_t W = 640, H = 480;
  camera.create_frame_ring<Camera>(W * H, 4);
  vision.attach_frame_ring<Camera>();

  std::atomic<int> processed{0};
  vision.subscribe_frame<Camera>([&](const Camera &meta, chappe::ShmSlotView &slot) {
    const uint8_t *px = static_cast<const uint8_t *>(slot.data());
    uint64_t sum = 0; // trivial "processing": average brightness, read in place
    for (size_t i = 0; i < slot.size(); i++)
      sum += px[i];
    std::cout << "[vision] frame ts=" << meta.timestamp_ns << " " << meta.width
              << "x" << meta.height << " avg=" << (sum / slot.size()) << "\n";
    processed.fetch_add(1);
  });
  vision.sync();

  std::cout << "-- camera publishing 5 frames --\n";
  for (int f = 0; f < 5; f++) {
    bool ok = camera.publish_frame<Camera>(
        /*ts=*/1000 + f, W, H, /*stride=*/W,
        [f](void *dst, size_t n) { std::memset(dst, 40 + f * 20, n); });
    if (!ok)
      std::cout << "[camera] frame " << f << " dropped (all slots held)\n";
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }

  for (int i = 0; i < 100 && processed.load() < 5; i++)
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  std::cout << "\nprocessed " << processed.load() << " frames\n";
}
