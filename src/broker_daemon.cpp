// src/broker_daemon.cpp — the broker daemon process.
// Usage: broker_daemon [socket-path]   (default /tmp/broker.sock)
#include "broker_server.hpp"
#include <csignal>
#include <iostream>
#include <pthread.h>
#include <string>

int main(int argc, char **argv) {
  std::string path = (argc > 1) ? argv[1] : ipc::default_broker_addr();

  // Block SIGINT/SIGTERM here so accept/reader threads inherit the mask and the
  // signal is delivered to sigwait below for a clean shutdown.
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGINT);
  sigaddset(&set, SIGTERM);
  pthread_sigmask(SIG_BLOCK, &set, nullptr);

  try {
    ipc::BrokerServer server(path);
    std::cout << "broker listening on " << path << " (SIGINT to stop)\n";
    int sig = 0;
    sigwait(&set, &sig);
    std::cout << "\nshutting down\n";
  } catch (const std::exception &e) {
    std::cerr << "broker error: " << e.what() << "\n";
    return 1;
  }
  return 0;
}
