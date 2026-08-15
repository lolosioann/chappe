// examples/consumer.cpp
// Cross-process pub/sub, subscriber side. Start the daemon (./bin/chappe_daemon),
// run this, then run ./bin/producer in another terminal.
//   ./bin/consumer [broker-socket]     (defaults to the well-known address)
#include "node.hpp"
#include "tick.hpp"
#include <chrono>
#include <iostream>
#include <string>
#include <thread>

int main(int argc, char **argv) {
  std::cout << std::unitbuf; // flush each line: this process is Ctrl-C'd, never exits cleanly
  std::string addr = (argc > 1) ? std::string(argv[1]) : chappe::default_addr();

  chappe::Node consumer("consumer");
  consumer.connect(addr);

  consumer.subscribe([](const Tick &t) {
    std::cout << "[consumer] got tick " << t.seq << "\n";
  });

  std::cout << "[consumer] listening for ticks (Ctrl-C to stop)\n";
  for (;;) // handlers run on the reader thread; just keep the process alive
    std::this_thread::sleep_for(std::chrono::seconds(1));
}
