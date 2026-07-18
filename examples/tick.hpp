#pragma once
#include "broker.hpp"

// Shared message contract. Both the producer and consumer processes include
// this so they agree on the topic name and wire layout — the only thing two
// separate programs need to share to talk over the broker.
struct Tick {
  int seq;
};
MAKE_TOPIC(Tick, "tick");
