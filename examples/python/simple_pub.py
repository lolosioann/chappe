import time

from chappe import Node


def main():
    """
    Simple publish loop. Keeps publishing the 'hello' message
    every second until Ctrl-C is pressed
    """
    simple_pub_node = Node("node_name")
    try:
        simple_pub_node.connect()
    except Exception:
        print("Could not connect to daemon. Make sure chappe is installed and running")
        return
    while True:
        simple_pub_node.publish("example_pub_topic", "Hello from Chappe!")
        time.sleep(1)


if __name__ == "__main__":
    main()
