// tests/test_frame_ipc.cpp
#include "broker.hpp"
#include "ipc/frame_handle.hpp"
#include "node.hpp"
#include "test.hpp"
#include <atomic>
#include <cstring>
#include <sys/mman.h>

// ---- Frame topics ----------------------------------------------------------
// Each frame topic is its own broker message type, derived from ipc::FrameHandle.

struct FrontCam : ipc::FrameHandle {};
struct RearCam : ipc::FrameHandle {};
struct SideCam : ipc::FrameHandle {};

MAKE_TOPIC(FrontCam, "cam.front");
MAKE_TOPIC(RearCam, "cam.rear");
MAKE_TOPIC(SideCam, "cam.side");

static void clean(const char *shm) { shm_unlink(shm); }

// ---- Tests -----------------------------------------------------------------

void test_frame_sync() {
  clean("/broker_cam.front");
  Broker<FrontCam> broker;
  Node<FrontCam> producer("prod", broker);
  Node<FrontCam> consumer("cons", broker);
  producer.create_frame_ring<FrontCam>(16, 4);
  consumer.attach_frame_ring<FrontCam>();

  bool called = false;
  FrontCam got{};
  unsigned char pix[16] = {0};
  consumer.subscribe_frame<FrontCam>(
      [&](const FrontCam &fh, ipc::ShmSlotView &v) {
        called = true;
        got = fh;
        std::memcpy(pix, v.data(), v.size());
      });

  bool ok = producer.publish_frame<FrontCam>(
      12345, 4, 4, 4, [](void *d, size_t n) {
        for (size_t i = 0; i < n; i++)
          static_cast<unsigned char *>(d)[i] = (unsigned char)(i + 1);
      });

  ASSERT_TRUE(ok);
  ASSERT_TRUE(called);
  ASSERT_EQ(got.timestamp_ns, (uint64_t)12345);
  ASSERT_EQ(got.width, (uint32_t)4);
  ASSERT_EQ((int)pix[0], 1);
  ASSERT_EQ((int)pix[15], 16);
}

void test_frame_async() {
  clean("/broker_cam.rear");
  Broker<RearCam> broker;
  Node<RearCam> producer("prod", broker);
  Node<RearCam> consumer("cons", broker, 2); // 2 worker threads
  producer.create_frame_ring<RearCam>(8, 4);
  consumer.attach_frame_ring<RearCam>();

  std::atomic<bool> called{false};
  std::atomic<uint64_t> got_ts{0};
  std::atomic<int> first_byte{-1};
  consumer.subscribe_frame<RearCam>(
      [&](const RearCam &fh, ipc::ShmSlotView &v) {
        got_ts.store(fh.timestamp_ns);
        first_byte.store((int)static_cast<unsigned char *>(v.data())[0]);
        called.store(true);
      });

  producer.publish_frame<RearCam>(
      777, 2, 2, 2, [](void *d, size_t n) { std::memset(d, 0xAB, n); });
  consumer.drain();

  ASSERT_TRUE(called.load());
  ASSERT_EQ(got_ts.load(), (uint64_t)777);
  ASSERT_EQ(first_byte.load(), 0xAB);
}

void test_frame_drop_no_ring() {
  clean("/broker_cam.side");
  Broker<SideCam> broker;
  Node<SideCam> producer("prod", broker);
  Node<SideCam> consumer("cons", broker); // subscribes but never attaches
  producer.create_frame_ring<SideCam>(8, 4);

  consumer.subscribe_frame<SideCam>(
      [](const SideCam &, ipc::ShmSlotView &) { /* never reached */ });

  producer.publish_frame<SideCam>(
      1, 1, 1, 1, [](void *d, size_t n) { std::memset(d, 0, n); });

  ASSERT_EQ(consumer.frame_drops(), (uint64_t)1);
}

int main() {
  test_case("frame sync publish/subscribe", test_frame_sync);
  test_case("frame async (threadpool)", test_frame_async);
  test_case("frame drop when no ring attached", test_frame_drop_no_ring);
  return test_summary();
}
