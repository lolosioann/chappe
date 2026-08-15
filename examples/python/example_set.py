from chappe import Node


def main():
    example_node = Node("name")
    try:
        example_node.connect()
    except:
        print("could not connect to darmon")
        return
    counter = 58
    example_node.set("counter", counter)
    print("Set counter = 42")


if __name__ == "__main__":
    main()
