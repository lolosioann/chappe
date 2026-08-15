// examples/producer.cpp
// Cross-process pub/sub, publisher side. Start the daemon (./bin/chappe_daemon),
// then run ./bin/consumer in one terminal and ./bin/producer in another.
//   ./bin/producer [broker-socket]     (defaults to the well-known address)
#include "node.hpp"
#include "tick.hpp"
#include <chrono>
#include <iostream>
#include <string>
#include <thread>

int main(int argc, char **argv) {
  std::string addr = (argc > 1) ? std::string(argv[1]) : chappe::default_addr();

  chappe::Node producer("producer");
  producer.connect(addr);

  for (int i = 1; i <= 10; i++) {
    producer.publish(Tick{i});
    std::cout << "[producer] sent tick " << i << "\n";
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }
  std::cout << "[producer] done\n";
}
