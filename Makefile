CXX      := g++
CXXFLAGS := -std=c++17 -pthread -Wall -Wextra -Iinclude
BINDIR   := bin

# ---- targets ---------------------------------------------------------------

ALL_TESTS := $(BINDIR)/test_broker $(BINDIR)/test_threadpool $(BINDIR)/test_node
ALL_EXAMPLES := $(BINDIR)/basic

.PHONY: all test examples clean \
        test_broker test_threadpool test_node

all: test examples

# ---- test binaries ---------------------------------------------------------

$(BINDIR)/test_broker: tests/test_broker.cpp include/broker.hpp include/test.hpp
	@mkdir -p $(BINDIR)
	$(CXX) $(CXXFLAGS) $< -o $@

$(BINDIR)/test_threadpool: tests/test_threadpool.cpp include/threadpool.hpp include/test.hpp
	@mkdir -p $(BINDIR)
	$(CXX) $(CXXFLAGS) $< -o $@

$(BINDIR)/test_node: tests/test_node.cpp include/node.hpp include/broker.hpp include/threadpool.hpp include/test.hpp
	@mkdir -p $(BINDIR)
	$(CXX) $(CXXFLAGS) $< -o $@

# ---- run individual test suites --------------------------------------------

test_broker: $(BINDIR)/test_broker
	@echo "\n========== test_broker =========="; ./$(BINDIR)/test_broker

test_threadpool: $(BINDIR)/test_threadpool
	@echo "\n========== test_threadpool =========="; ./$(BINDIR)/test_threadpool

test_node: $(BINDIR)/test_node
	@echo "\n========== test_node =========="; ./$(BINDIR)/test_node

# ---- run all tests ---------------------------------------------------------

test: $(ALL_TESTS)
	@echo "\n========== test_broker =========="
	@./$(BINDIR)/test_broker;      status1=$$?; \
	echo "\n========== test_threadpool =========="; \
	./$(BINDIR)/test_threadpool;   status2=$$?; \
	echo "\n========== test_node =========="; \
	./$(BINDIR)/test_node;         status3=$$?; \
	echo "\n========== summary =========="; \
	if [ $$status1 -eq 0 ] && [ $$status2 -eq 0 ] && [ $$status3 -eq 0 ]; then \
		echo "all suites passed"; exit 0; \
	else \
		echo "one or more suites failed"; exit 1; \
	fi

# ---- examples --------------------------------------------------------------

$(BINDIR)/basic: examples/basic.cpp include/broker.hpp include/node.hpp include/threadpool.hpp
	@mkdir -p $(BINDIR)
	$(CXX) $(CXXFLAGS) $< -o $@

examples: $(BINDIR)/basic
	@echo "\n========== basic example =========="; ./$(BINDIR)/basic

# ---- clean -----------------------------------------------------------------

clean:
	rm -rf $(BINDIR)
