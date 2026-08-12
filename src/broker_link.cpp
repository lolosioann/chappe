// src/broker_link.cpp — joins this device's bus to one peer device's bus.
//
// One side listens, the other connects; they are otherwise symmetric. Run one
// per peer:
//   device A:  broker_link --listen 0.0.0.0:7000 --topic 'cmd/*' --key state/mode
//   device B:  broker_link --peer  a.local:7000  --topic 'cmd/*' --key state/mode
//
// No auth on the wire — put links on a private network. See include/link.hpp.
#include "link.hpp"
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <pthread.h>
#include <string>

static void usage(const char *me) {
  std::cerr
      << "usage: " << me << " (--listen ADDR:PORT | --peer HOST:PORT) [options]\n"
      << "  --socket PATH     local broker socket (default $BROKER_SOCKET)\n"
      << "  --topic PATTERN   topic forwarded both ways, wildcards ok "
         "(repeatable)\n"
      << "  --key NAME        kv key forwarded both ways, exact name "
         "(repeatable)\n"
      << "\nDon't list a frame topic: a FrameHandle names shared memory on the\n"
         "host that published it, so forwarding one points the far side at a\n"
         "segment that is missing, or worse, a different ring of the same "
         "name.\n";
}

// "host:port" — rightmost colon, so a bare IPv6 address still splits correctly.
static bool split_addr(const std::string &s, std::string &host, uint16_t &port) {
  auto c = s.rfind(':');
  if (c == std::string::npos || c + 1 >= s.size())
    return false;
  host = s.substr(0, c);
  port = static_cast<uint16_t>(std::atoi(s.c_str() + c + 1));
  return port != 0;
}

int main(int argc, char **argv) {
  ipc::BrokerLink::Config cfg;
  std::string listen_addr, peer_addr;
  for (int i = 1; i < argc; i++) {
    std::string a = argv[i];
    bool has_next = i + 1 < argc;
    if (a == "--socket" && has_next)
      cfg.socket = argv[++i];
    else if (a == "--topic" && has_next)
      cfg.topics.push_back(argv[++i]);
    else if (a == "--key" && has_next)
      cfg.keys.push_back(argv[++i]);
    else if (a == "--listen" && has_next)
      listen_addr = argv[++i];
    else if (a == "--peer" && has_next)
      peer_addr = argv[++i];
    else {
      usage(argv[0]);
      return 2;
    }
  }
  if (listen_addr.empty() == peer_addr.empty()) { // need exactly one
    usage(argv[0]);
    return 2;
  }
  if (cfg.topics.empty() && cfg.keys.empty()) {
    std::cerr << "nothing to forward: pass at least one --topic or --key\n";
    return 2;
  }

  std::string host;
  uint16_t port = 0;
  const std::string &addr = listen_addr.empty() ? peer_addr : listen_addr;
  if (!split_addr(addr, host, port)) {
    std::cerr << "bad address '" << addr << "', want HOST:PORT\n";
    return 2;
  }

  // Block the signals here so the link's threads inherit the mask and sigwait
  // below owns the shutdown, exactly as broker_daemon does.
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGINT);
  sigaddset(&set, SIGTERM);
  pthread_sigmask(SIG_BLOCK, &set, nullptr);

  int peer_fd = -1;
  if (!listen_addr.empty()) {
    int srv = ipc::tcp_listen(host, port);
    if (srv < 0) {
      std::cerr << "cannot listen on " << addr << "\n";
      return 1;
    }
    std::cout << "waiting for a peer link on " << addr << "\n";
    peer_fd = ipc::tcp_accept(srv);
    ::close(srv); // one peer per process; supervision handles more
  } else {
    peer_fd = ipc::tcp_connect(host, port);
  }
  if (peer_fd < 0) {
    std::cerr << "no peer link at " << addr << "\n";
    return 1;
  }

  try {
    ipc::BrokerLink link(cfg, peer_fd);
    std::cout << "linked: " << cfg.topics.size() << " topic pattern(s), "
              << cfg.keys.size() << " key(s) (SIGINT to stop)\n";
    // Wake periodically so a peer that hangs up ends the process too, rather
    // than leaving a link that forwards nothing until someone notices.
    while (link.alive()) {
      timespec ts{1, 0};
      int sig = 0;
      if (sigtimedwait(&set, nullptr, &ts) > 0) {
        (void)sig;
        std::cout << "\nshutting down\n";
        return 0;
      }
    }
    std::cerr << "peer link lost\n";
  } catch (const std::exception &e) {
    std::cerr << "link error: " << e.what() << "\n";
    return 1;
  }
  return 1; // only reached when the peer dropped: non-zero so we get restarted
}
