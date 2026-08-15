// examples/keyvalue.cpp
// The get/set transport: an authoritative store in the daemon with a
// read-through cache on each client. A config node writes parameters; a worker
// node reads them and sees live updates pushed into its cache.
#include "server.hpp"
#include "node.hpp"
#include <chrono>
#include <iostream>
#include <string>
#include <thread>

int main() {
  chappe::Server broker; // in-process here; real use: run chappe_daemon

  chappe::Node config("config");
  chappe::Node worker("worker");
  config.connect();
  worker.connect();

  // Authoritative writes land in the daemon. sync() makes sure they're applied
  // before the worker's first read.
  config.set<float>("max_speed", 2.5f);
  config.set<std::string>("mode", "race");
  config.set<int>("gear", 3);
  config.sync();

  // First read of each key round-trips to the daemon and starts watching it.
  std::cout << "max_speed = " << worker.get<float>("max_speed").value() << "\n";
  std::cout << "mode      = " << worker.get<std::string>("mode").value() << "\n";
  std::cout << "gear      = " << worker.get<int>("gear").value() << "\n";

  // Unknown key -> nullopt.
  auto limit = worker.get<int>("throttle_limit");
  std::cout << "throttle_limit set? " << (limit ? "yes" : "no") << "\n";

  // A later write by any node is pushed into the worker's cache (it's watching
  // "mode" after the read above), so subsequent local reads reflect it.
  std::cout << "\n-- config changes mode to 'safe' --\n";
  config.set<std::string>("mode", "safe");

  std::string mode;
  for (int i = 0; i < 200; i++) {
    mode = worker.get<std::string>("mode").value_or(""); // warm: reads cache
    if (mode == "safe")
      break;
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  std::cout << "worker now sees mode = " << mode << "\n";
}
