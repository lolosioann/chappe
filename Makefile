CXX      := g++
CXXFLAGS := -std=c++17 -pthread -Wall -Wextra -Iinclude
CC       := cc
CFLAGS   := -std=c11 -pthread -Wall -Wextra -Iinclude
BINDIR   := bin

# node.hpp pulls in the shm ring (frame API), so anything using Node depends on
# these headers and must link shm_ring.o.
NODE_DEPS := include/node.hpp include/broker.hpp include/threadpool.hpp \
             include/ipc/shm_ring.hpp include/ipc/frame_handle.hpp include/shm_ring.h

# ---- targets ---------------------------------------------------------------

ALL_TESTS := $(BINDIR)/test_broker $(BINDIR)/test_threadpool $(BINDIR)/test_node \
             $(BINDIR)/test_frame_ipc $(BINDIR)/test_transport
ALL_EXAMPLES := $(BINDIR)/basic

.PHONY: all test examples clean \
        test_broker test_threadpool test_node test_frame_ipc test_transport \
        test_shm_ring stress_shm_ring bench_shm_ring

all: test examples

# ---- test binaries ---------------------------------------------------------

$(BINDIR)/test_broker: tests/test_broker.cpp include/broker.hpp include/test.hpp
	@mkdir -p $(BINDIR)
	$(CXX) $(CXXFLAGS) $< -o $@

$(BINDIR)/test_threadpool: tests/test_threadpool.cpp include/threadpool.hpp include/test.hpp
	@mkdir -p $(BINDIR)
	$(CXX) $(CXXFLAGS) $< -o $@

$(BINDIR)/test_node: tests/test_node.cpp $(NODE_DEPS) include/test.hpp $(BINDIR)/shm_ring.o
	@mkdir -p $(BINDIR)
	$(CXX) $(CXXFLAGS) $< $(BINDIR)/shm_ring.o -o $@ -lrt

$(BINDIR)/test_frame_ipc: tests/test_frame_ipc.cpp $(NODE_DEPS) include/test.hpp $(BINDIR)/shm_ring.o
	@mkdir -p $(BINDIR)
	$(CXX) $(CXXFLAGS) $< $(BINDIR)/shm_ring.o -o $@ -lrt

$(BINDIR)/test_transport: tests/test_transport.cpp $(NODE_DEPS) include/ipc/transport.hpp include/test.hpp $(BINDIR)/shm_ring.o
	@mkdir -p $(BINDIR)
	$(CXX) $(CXXFLAGS) $< $(BINDIR)/shm_ring.o -o $@ -lrt

# ---- run individual test suites --------------------------------------------

test_broker: $(BINDIR)/test_broker
	@echo "\n========== test_broker =========="; ./$(BINDIR)/test_broker

test_threadpool: $(BINDIR)/test_threadpool
	@echo "\n========== test_threadpool =========="; ./$(BINDIR)/test_threadpool

test_node: $(BINDIR)/test_node
	@echo "\n========== test_node =========="; ./$(BINDIR)/test_node

test_frame_ipc: $(BINDIR)/test_frame_ipc
	@echo "\n========== test_frame_ipc =========="; ./$(BINDIR)/test_frame_ipc

test_transport: $(BINDIR)/test_transport
	@echo "\n========== test_transport =========="; ./$(BINDIR)/test_transport

# ---- run all tests ---------------------------------------------------------

test: $(ALL_TESTS)
	@echo "\n========== test_broker =========="
	@./$(BINDIR)/test_broker;      status1=$$?; \
	echo "\n========== test_threadpool =========="; \
	./$(BINDIR)/test_threadpool;   status2=$$?; \
	echo "\n========== test_node =========="; \
	./$(BINDIR)/test_node;         status3=$$?; \
	echo "\n========== test_frame_ipc =========="; \
	./$(BINDIR)/test_frame_ipc;    status4=$$?; \
	echo "\n========== test_transport =========="; \
	./$(BINDIR)/test_transport;    status5=$$?; \
	echo "\n========== summary =========="; \
	if [ $$status1 -eq 0 ] && [ $$status2 -eq 0 ] && [ $$status3 -eq 0 ] && [ $$status4 -eq 0 ] && [ $$status5 -eq 0 ]; then \
		echo "all suites passed"; exit 0; \
	else \
		echo "one or more suites failed"; exit 1; \
	fi

# ---- shm_ring (C) ----------------------------------------------------------

# Object for linking into C++ consumers (via the extern "C" header).
$(BINDIR)/shm_ring.o: src/shm_ring.c include/shm_ring.h
	@mkdir -p $(BINDIR)
	$(CC) $(CFLAGS) -c $< -o $@

$(BINDIR)/test_shm_ring: src/shm_ring.c include/shm_ring.h
	@mkdir -p $(BINDIR)
	$(CC) $(CFLAGS) -DSHM_RING_TEST $< -o $@ -lrt

test_shm_ring: $(BINDIR)/test_shm_ring
	@echo "\n========== test_shm_ring =========="; ./$(BINDIR)/test_shm_ring

# Fork-based concurrency stress test (-O2: real reordering pressure).
$(BINDIR)/stress_shm_ring: tests/stress_shm_ring.c src/shm_ring.c include/shm_ring.h
	@mkdir -p $(BINDIR)
	$(CC) $(CFLAGS) -O2 tests/stress_shm_ring.c src/shm_ring.c -o $@ -lrt

stress_shm_ring: $(BINDIR)/stress_shm_ring
	@echo "\n========== stress_shm_ring =========="; ./$(BINDIR)/stress_shm_ring

$(BINDIR)/bench_shm_ring: tests/bench_shm_ring.c src/shm_ring.c include/shm_ring.h
	@mkdir -p $(BINDIR)
	$(CC) $(CFLAGS) -O2 tests/bench_shm_ring.c src/shm_ring.c -o $@ -lrt

bench_shm_ring: $(BINDIR)/bench_shm_ring
	@echo "\n========== bench_shm_ring =========="; ./$(BINDIR)/bench_shm_ring

# ---- examples --------------------------------------------------------------

$(BINDIR)/basic: examples/basic.cpp $(NODE_DEPS) $(BINDIR)/shm_ring.o
	@mkdir -p $(BINDIR)
	$(CXX) $(CXXFLAGS) $< $(BINDIR)/shm_ring.o -o $@ -lrt

examples: $(BINDIR)/basic
	@echo "\n========== basic example =========="; ./$(BINDIR)/basic

# ---- clean -----------------------------------------------------------------

clean:
	rm -rf $(BINDIR)
