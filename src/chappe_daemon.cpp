// src/chappe_daemon.cpp — the broker daemon process.
// Usage: chappe_daemon [socket-path]   (default /tmp/chappe.sock)
#include "server.hpp"
#include <csignal>
#include <iostream>
#include <pthread.h>
#include <string>

int main(int argc, char **argv) {
  std::string arg = (argc > 1) ? argv[1] : "";
  if (arg == "-h" || arg == "--help") {
    std::cout
        << "usage: " << argv[0] << " [socket-path]\n"
        << "  socket-path   where to listen (default $CHAPPE_SOCKET, else "
        << chappe::default_addr() << ")\n\n"
        << "The socket is created 0600 and every connection is checked against\n"
           "SO_PEERCRED, so clients must run as the same user. SIGINT/SIGTERM\n"
           "shut down and unlink it.\n";
    return 0;
  }
  // Anything else is the path, including a stray flag: without the check above
  // `--help` was a perfectly good socket name, and you got a running daemon
  // nobody could find.
  std::string path = arg.empty() ? chappe::default_addr() : arg;

  // Block SIGINT/SIGTERM here so accept/reader threads inherit the mask and the
  // signal is delivered to sigwait below for a clean shutdown.
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGINT);
  sigaddset(&set, SIGTERM);
  pthread_sigmask(SIG_BLOCK, &set, nullptr);

  try {
    chappe::Server server(path);
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
