import time

from chappe import Node


def main():
    example_node = Node("name")
    try:
        example_node.connect()
    except:
        print("could not connect to darmon")
        return
    while True:
        counter = example_node.get("counter")
        print(counter)
        time.sleep(1)


if __name__ == "__main__":
    main()
