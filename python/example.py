#!/usr/bin/env python3
"""Python client demo. Start the daemon first:  ./bin/broker_daemon

Shows pub/sub and get/set from Python. The "tick" payload is packed the same way
the C++ side encodes `struct Tick { int seq; }`, so this also interoperates with
the C++ examples: run ./bin/producer and this consumer will decode its ticks.
"""
import struct
import time

from broker import Node


def main():
    with Node("py-consumer") as consumer, Node("py-producer") as producer:
        consumer.connect()
        producer.connect()

        got = []
        consumer.subscribe("tick", lambda p: got.append(struct.unpack("=i", p)[0]))
        consumer.sync()  # subscription is live before we publish

        for i in range(1, 4):
            producer.publish("tick", struct.pack("=i", i))
        time.sleep(0.2)
        print("consumer received ticks:", got)

        # get/set store
        producer.set("mode", b"race")
        producer.sync()
        print("mode    =", consumer.get("mode"))
        print("missing =", consumer.get("nope"))


if __name__ == "__main__":
    main()
