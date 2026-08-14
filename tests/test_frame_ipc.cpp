// tests/test_frame_ipc.cpp
// Frames: the small FrameHandle is routed through the daemon; the pixel bytes
// move producer->consumer directly through the shm ring.
#include "chappe.hpp"
#include "server.hpp"
#include "ipc/frame_handle.hpp"
#include "node.hpp"
#include "test.hpp"
#include <atomic>
#include <chrono>
#include <cstring>
#include <string>
#include <sys/mman.h>
#include <thread>
#include <unistd.h>

using namespace chappe;

// ---- Frame topics ----------------------------------------------------------

struct FrontCam : chappe::FrameHandle {};
struct RearCam : chappe::FrameHandle {};
struct SideCam : chappe::FrameHandle {};

MAKE_TOPIC(FrontCam, "cam.front");
MAKE_TOPIC(RearCam, "cam.rear");
MAKE_TOPIC(SideCam, "cam.side");

static void clean(const char *shm) { shm_unlink(shm); }

static std::string sock_path(const char *tag) {
  return std::string("/tmp/chappe_frame_") + tag + "_" +
         std::to_string(::getpid()) + ".sock";
}

template <typename P> static bool wait_until(P pred) {
  for (int i = 0; i < 400; i++) {
    if (pred())
      return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  return pred();
}

// ---- Tests -----------------------------------------------------------------

void test_frame_sync() {
  clean("/chappe_cam.front");
  auto p = sock_path("front");
  chappe::Server server(p);
  Node producer("prod");
  Node consumer("cons");
  producer.connect(p);
  consumer.connect(p);
  producer.create_frame_ring<FrontCam>(16, 4);
  consumer.attach_frame_ring<FrontCam>();

  std::atomic<bool> called{false};
  FrontCam got{};
  unsigned char pix[16] = {0};
  consumer.subscribe_frame<FrontCam>(
      [&](const FrontCam &fh, chappe::ShmSlotView &v) {
        got = fh;
        std::memcpy(pix, v.data(), v.size());
        called.store(true); // release: publishes got/pix to the reader below
      });
  consumer.sync();

  bool ok = producer.publish_frame<FrontCam>(
      12345, 4, 4, 4, [](void *d, size_t n) {
        for (size_t i = 0; i < n; i++)
          static_cast<unsigned char *>(d)[i] = (unsigned char)(i + 1);
      });

  ASSERT_TRUE(ok);
  ASSERT_TRUE(wait_until([&] { return called.load(); }));
  ASSERT_EQ(got.timestamp_ns, (uint64_t)12345);
  ASSERT_EQ(got.width, (uint32_t)4);
  ASSERT_EQ((int)pix[0], 1);
  ASSERT_EQ((int)pix[15], 16);
}

void test_frame_async() {
  clean("/chappe_cam.rear");
  auto p = sock_path("rear");
  chappe::Server server(p);
  Node producer("prod");
  Node consumer("cons", 2); // 2 worker threads
  producer.connect(p);
  consumer.connect(p);
  producer.create_frame_ring<RearCam>(8, 4);
  consumer.attach_frame_ring<RearCam>();

  std::atomic<bool> called{false};
  std::atomic<uint64_t> got_ts{0};
  std::atomic<int> first_byte{-1};
  consumer.subscribe_frame<RearCam>(
      [&](const RearCam &fh, chappe::ShmSlotView &v) {
        got_ts.store(fh.timestamp_ns);
        first_byte.store((int)static_cast<unsigned char *>(v.data())[0]);
        called.store(true);
      });
  consumer.sync();

  producer.publish_frame<RearCam>(
      777, 2, 2, 2, [](void *d, size_t n) { std::memset(d, 0xAB, n); });

  ASSERT_TRUE(wait_until([&] { return called.load(); }));
  consumer.drain();
  ASSERT_EQ(got_ts.load(), (uint64_t)777);
  ASSERT_EQ(first_byte.load(), 0xAB);
}

void test_frame_drop_no_ring() {
  clean("/chappe_cam.side");
  auto p = sock_path("side");
  chappe::Server server(p);
  Node producer("prod");
  Node consumer("cons"); // subscribes but never attaches
  producer.connect(p);
  consumer.connect(p);
  producer.create_frame_ring<SideCam>(8, 4);

  consumer.subscribe_frame<SideCam>(
      [](const SideCam &, chappe::ShmSlotView &) { /* never reached */ });
  consumer.sync();

  producer.publish_frame<SideCam>(
      1, 1, 1, 1, [](void *d, size_t n) { std::memset(d, 0, n); });

  ASSERT_TRUE(wait_until([&] { return consumer.frame_drops() == 1; }));
  ASSERT_EQ(consumer.frame_drops(), (uint64_t)1);
}

struct AbandonCam : chappe::FrameHandle {};
MAKE_TOPIC(AbandonCam, "cam/abandon");

// A writer that throws mid-fill must return its slot to the free pool, not leak
// it as WRITING. With a 2-slot ring, a leak would starve after two throws; with
// the abandon path, any number of throwing publishes leave the ring usable.
void test_frame_writer_throws_no_leak() {
  clean("/chappe_cam_abandon");
  auto p = sock_path("abandon");
  chappe::Server server(p);
  Node producer("prod");
  producer.connect(p);
  producer.create_frame_ring<AbandonCam>(16, 2); // only 2 slots

  for (int i = 0; i < 5; i++) {
    bool threw = false;
    try {
      producer.publish_frame<AbandonCam>(
          i, 1, 1, 1, [](void *, size_t) { throw std::runtime_error("boom"); });
    } catch (const std::runtime_error &) {
      threw = true; // writer ran and threw -> slot must have been abandoned
    }
    ASSERT_TRUE(threw); // if a leak starved the ring, acquire would return
                        // false and the writer would never run (threw==false)
  }

  bool ok = producer.publish_frame<AbandonCam>(
      99, 1, 1, 1, [](void *d, size_t n) { std::memset(d, 7, n); });
  ASSERT_TRUE(ok); // ring still usable after five throwing publishes
}

int main() {
  test_case("frame sync publish/subscribe", test_frame_sync);
  test_case("frame async (threadpool)", test_frame_async);
  test_case("frame drop when no ring attached", test_frame_drop_no_ring);
  test_case("throwing writer abandons its slot", test_frame_writer_throws_no_leak);
  return test_summary();
}
