// Benchmarks for the broker layer (daemon routing), as opposed to the raw shm
// ring (see bench_shm_ring). Loopback: an in-process Server with client
// Nodes over a real unix socket — so this measures framing + socket + daemon
// routing, just without cross-process scheduling noise.
//
//   make bench_chappe
//   ./bin/bench_chappe
#include "chappe.hpp"
#include "server.hpp"
#include "node.hpp"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <sys/mman.h>
#include <unistd.h>
#include <vector>

using namespace chappe;

using clk = std::chrono::steady_clock;
static double us(clk::duration d) {
  return std::chrono::duration<double, std::micro>(d).count();
}
static double secs(clk::duration d) {
  return std::chrono::duration<double>(d).count();
}

struct Ping { uint32_t seq; };
struct Pong { uint32_t seq; };
struct Data { char buf[32]; };
struct BFrame : chappe::FrameHandle {};
MAKE_TOPIC(Ping, "ping");
MAKE_TOPIC(Pong, "pong");
MAKE_TOPIC(Data, "data");
MAKE_TOPIC(BFrame, "bench.frame");

static void report_lat(const char *name, std::vector<double> &v) {
  std::sort(v.begin(), v.end());
  auto pct = [&](double p) { return v[(size_t)(p * (v.size() - 1))]; };
  double mean = 0;
  for (double x : v)
    mean += x;
  mean /= v.size();
  printf("  %-22s n=%-7zu p50=%7.2f  p90=%7.2f  p99=%7.2f  mean=%7.2f us\n",
         name, v.size(), pct(0.50), pct(0.90), pct(0.99), mean);
}

// pub/sub round-trip: a publishes Ping, b echoes Pong, a times the loop.
static void bench_pubsub_latency(const std::string &sock) {
  Node a("a"), b("b");
  a.connect(sock);
  b.connect(sock);
  std::atomic<uint32_t> rx{0};
  b.subscribe([&b](const Ping &p) { b.publish(Pong{p.seq}); });
  a.subscribe([&rx](const Pong &p) { rx.store(p.seq, std::memory_order_release); });
  a.sync();
  b.sync();

  const uint32_t N = 20000, warm = 1000;
  std::vector<double> lat;
  lat.reserve(N);
  for (uint32_t i = 1; i <= N + warm; i++) {
    auto t0 = clk::now();
    a.publish(Ping{i});
    while (rx.load(std::memory_order_acquire) != i) // busy-poll for the echo
      ;
    auto dt = clk::now() - t0;
    if (i > warm)
      lat.push_back(us(dt));
  }
  report_lat("pub/sub RTT", lat);
}

// one publisher floods small messages; one subscriber counts.
static void bench_pubsub_throughput(const std::string &sock) {
  Node a("pub"), b("sub");
  a.connect(sock);
  b.connect(sock);
  const int N = 200000;
  std::atomic<int> cnt{0};
  b.subscribe([&cnt](const Data &) { cnt.fetch_add(1, std::memory_order_relaxed); });
  b.sync();

  Data d{};
  std::memset(d.buf, 0xAB, sizeof(d.buf));
  auto t0 = clk::now();
  for (int i = 0; i < N; i++)
    a.publish(d);
  while (cnt.load(std::memory_order_acquire) < N)
    ;
  double s = secs(clk::now() - t0);
  printf("  %-22s %d msgs, %.3fs => %8.1f k msgs/s  (%.1f MB/s payload)\n",
         "pub/sub throughput", N, s, N / s / 1e3,
         (double)N * sizeof(Data) / s / 1e6);
}

static void bench_kv(const std::string &sock) {
  Node w("w"), r("r");
  w.connect(sock);
  r.connect(sock);
  const int M = 10000;
  std::vector<std::string> keys(M);
  for (int i = 0; i < M; i++) {
    keys[i] = "k" + std::to_string(i);
    w.set<int>(keys[i], i);
  }
  w.sync();

  std::vector<double> cold, warm;
  cold.reserve(M);
  warm.reserve(M);
  for (int i = 0; i < M; i++) { // fresh key each time -> round-trips
    auto t0 = clk::now();
    volatile auto v = r.get<int>(keys[i]);
    cold.push_back(us(clk::now() - t0));
    (void)v;
  }
  for (int i = 0; i < M; i++) { // same key -> served from cache
    auto t0 = clk::now();
    volatile auto v = r.get<int>(keys[0]);
    warm.push_back(us(clk::now() - t0));
    (void)v;
  }
  report_lat("kv get cold (RTT)", cold);
  report_lat("kv get warm (cache)", warm);

  auto t0 = clk::now();
  for (int i = 0; i < M; i++)
    w.set<int>(keys[i], i + 1);
  w.sync();
  double s = secs(clk::now() - t0);
  printf("  %-22s %d sets, %.3fs => %8.1f k sets/s\n", "kv set throughput", M, s,
         M / s / 1e3);
}

// KV set throughput with W writer nodes running concurrently while R watcher
// nodes hold every key warm. Each set fans out R ways with kv_mu_ held across
// the sends, so W is the lock-contention axis and R the work-under-lock one.
// Writers own disjoint keys: this measures the lock, not key contention.
static void bench_kv_contention(const std::string &sock, int W, int R) {
  const int K = 64; // keys per writer — a node publishes a handful of state keys
  // Hold total fan-out roughly constant so a big config doesn't run for minutes.
  int N = (int)(2000000LL / ((long long)W * std::max(R, 1)));
  N = std::max(2000, std::min(20000, N));

  std::string tag = "c" + std::to_string(W) + "x" + std::to_string(R) + "_";
  std::vector<std::unique_ptr<Node>> writers, watchers;
  for (int i = 0; i < W; i++) {
    writers.push_back(std::make_unique<Node>("w" + std::to_string(i)));
    writers.back()->connect(sock);
  }
  for (int i = 0; i < R; i++) {
    watchers.push_back(std::make_unique<Node>("r" + std::to_string(i)));
    watchers.back()->connect(sock);
  }
  auto key = [&](int w, int k) {
    return tag + std::to_string(w) + "_" + std::to_string(k);
  };
  for (int w = 0; w < W; w++)
    for (int k = 0; k < K; k++)
      writers[w]->set<int>(key(w, k), 0);
  for (auto &w : writers)
    w->sync();
  for (auto &r : watchers) // a cold get is what registers the watch
    for (int w = 0; w < W; w++)
      for (int k = 0; k < K; k++)
        r->get<int>(key(w, k));

  std::atomic<bool> go{false};
  std::vector<std::thread> th;
  for (int w = 0; w < W; w++)
    th.emplace_back([&, w] {
      while (!go.load(std::memory_order_acquire))
        ;
      for (int i = 0; i < N; i++)
        writers[w]->set<int>(key(w, i % K), i);
      writers[w]->sync(); // PONG means the daemon drained this writer's sets
    });
  auto t0 = clk::now();
  go.store(true, std::memory_order_release);
  for (auto &t : th)
    t.join();
  double s = secs(clk::now() - t0);

  double sets = (double)W * N;
  printf("  W=%-2d R=%-2d  %6d sets/writer  %.3fs => %7.1f k sets/s  "
         "(%7.1f k fan-out sends/s)\n",
         W, R, N, s, sets / s / 1e3, sets * R / s / 1e3);
}

static void bench_frames(const std::string &sock, uint32_t w, uint32_t h) {
  shm_unlink("/chappe_bench.frame");
  size_t sz = (size_t)w * h;
  Node prod("fp"), cons("fc");
  prod.connect(sock);
  cons.connect(sock);
  prod.create_frame_ring<BFrame>(sz, 4);
  cons.attach_frame_ring<BFrame>();

  std::atomic<int> cnt{0};
  cons.subscribe_frame<BFrame>([&cnt](const BFrame &, chappe::ShmSlotView &v) {
    volatile unsigned char x = *static_cast<const unsigned char *>(v.data());
    (void)x; // touch it so the read isn't elided
    cnt.fetch_add(1, std::memory_order_relaxed);
  });
  cons.sync();

  int N = (int)(2000000000ULL / sz);
  if (N < 2000)
    N = 2000;
  if (N > 200000)
    N = 200000;
  std::vector<char> src(sz, (char)0xCD);
  int published = 0;
  auto t0 = clk::now();
  for (int i = 0; i < N; i++)
    if (prod.publish_frame<BFrame>(i, w, h, w, [&](void *d, size_t n) {
          std::memcpy(d, src.data(), n);
        }))
      published++;
  // every sent FrameHandle ends up either read (cnt) or a consumer-side drop
  while (cnt.load(std::memory_order_acquire) + (int)cons.frame_drops() < published)
    ;
  double s = secs(clk::now() - t0);
  printf("  %4ux%-4u (%4zu KB)  %6.1f k frames/s  %6.2f GB/s  (%d sent, %llu "
         "reader-drops)\n",
         w, h, sz / 1024, published / s / 1e3, (double)published * sz / s / 1e9,
         published, (unsigned long long)cons.frame_drops());
}

int main() {
  std::string sock = "/tmp/chappe_bench_" + std::to_string(::getpid()) + ".sock";
  ::unlink(sock.c_str());
  chappe::Server server(sock);

  printf("== broker layer (loopback over unix socket) ==\n");
  bench_pubsub_latency(sock);
  bench_pubsub_throughput(sock);
  bench_kv(sock);

  printf("\n== kv set throughput under contention ==\n");
  for (int W : {1, 2, 4, 8, 16}) // writers scaling, fan-out fixed
    bench_kv_contention(sock, W, 4);
  printf("\n");
  for (int R : {0, 1, 4, 8, 16}) // fan-out scaling, writers fixed
    bench_kv_contention(sock, 4, R);

  printf("\n== frames (pixels via shm, FrameHandle via broker) ==\n");
  bench_frames(sock, 640, 480);
  bench_frames(sock, 1920, 1080);
  return 0;
}
