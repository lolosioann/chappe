import time

from chappe import Node


def callback_func(message_payload):
    print(message_payload)


def main():
    simple_sub_node = Node("node_name")
    simple_sub_node.subscribe("example_pub_topic", callback_func)
    try:
        simple_sub_node.connect()
    except Exception:
        print("Could not connect to daemon. Make sure chappe is installed and running")
        return
    while True:
        time.sleep(10)


if __name__ == "__main__":
    main()
