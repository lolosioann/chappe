CXX      := g++
CXXFLAGS := -std=c++17 -pthread -Wall -Wextra -Iinclude
CC       := cc
CFLAGS   := -std=c11 -pthread -Wall -Wextra -Iinclude
BINDIR   := bin

# Client (Node) headers. node.hpp pulls in the shm ring (frame API) and the
# wire layer, so anything using Node depends on these and links shm_ring.o.
CLIENT_DEPS := include/node.hpp include/broker.hpp include/threadpool.hpp \
               include/ipc/shm_ring.hpp include/ipc/frame_handle.hpp \
               include/ipc/transport.hpp include/shm_ring.h
# Daemon (BrokerServer) headers — no shm, sockets only.
SERVER_DEPS := include/broker_server.hpp include/ipc/transport.hpp include/broker.hpp

# ---- targets ---------------------------------------------------------------

ALL_TESTS := $(BINDIR)/test_threadpool $(BINDIR)/test_node \
             $(BINDIR)/test_frame_ipc $(BINDIR)/test_transport
# Self-contained demos (embedded daemon) — built and run by `make examples`.
SELF_EXAMPLES := $(BINDIR)/basic $(BINDIR)/keyvalue $(BINDIR)/frames
# Cross-process demo — built only; run by hand against a broker_daemon.
IPC_EXAMPLES  := $(BINDIR)/producer $(BINDIR)/consumer
ALL_EXAMPLES  := $(SELF_EXAMPLES) $(IPC_EXAMPLES)

.PHONY: all test examples daemon libshm_ring clean \
        test_threadpool test_node test_frame_ipc test_transport \
        test_shm_ring stress_shm_ring bench_shm_ring

all: test examples daemon libshm_ring

# ---- broker daemon ---------------------------------------------------------

daemon: $(BINDIR)/broker_daemon

$(BINDIR)/broker_daemon: src/broker_daemon.cpp $(SERVER_DEPS)
	@mkdir -p $(BINDIR)
	$(CXX) $(CXXFLAGS) $< -o $@

# ---- test binaries ---------------------------------------------------------

$(BINDIR)/test_threadpool: tests/test_threadpool.cpp include/threadpool.hpp include/test.hpp
	@mkdir -p $(BINDIR)
	$(CXX) $(CXXFLAGS) $< -o $@

$(BINDIR)/test_node: tests/test_node.cpp $(CLIENT_DEPS) $(SERVER_DEPS) include/test.hpp $(BINDIR)/shm_ring.o
	@mkdir -p $(BINDIR)
	$(CXX) $(CXXFLAGS) $< $(BINDIR)/shm_ring.o -o $@ -lrt

$(BINDIR)/test_frame_ipc: tests/test_frame_ipc.cpp $(CLIENT_DEPS) $(SERVER_DEPS) include/test.hpp $(BINDIR)/shm_ring.o
	@mkdir -p $(BINDIR)
	$(CXX) $(CXXFLAGS) $< $(BINDIR)/shm_ring.o -o $@ -lrt

$(BINDIR)/test_transport: tests/test_transport.cpp $(CLIENT_DEPS) $(SERVER_DEPS) include/test.hpp $(BINDIR)/shm_ring.o
	@mkdir -p $(BINDIR)
	$(CXX) $(CXXFLAGS) $< $(BINDIR)/shm_ring.o -o $@ -lrt

# ---- run individual test suites --------------------------------------------

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
	@echo "\n========== test_threadpool =========="
	@./$(BINDIR)/test_threadpool;  status1=$$?; \
	echo "\n========== test_node =========="; \
	./$(BINDIR)/test_node;         status2=$$?; \
	echo "\n========== test_frame_ipc =========="; \
	./$(BINDIR)/test_frame_ipc;    status3=$$?; \
	echo "\n========== test_transport =========="; \
	./$(BINDIR)/test_transport;    status4=$$?; \
	echo "\n========== summary =========="; \
	if [ $$status1 -eq 0 ] && [ $$status2 -eq 0 ] && [ $$status3 -eq 0 ] && [ $$status4 -eq 0 ]; then \
		echo "all suites passed"; exit 0; \
	else \
		echo "one or more suites failed"; exit 1; \
	fi

# ---- shm_ring (C) ----------------------------------------------------------

# Object for linking into C++ consumers (via the extern "C" header).
$(BINDIR)/shm_ring.o: src/shm_ring.c include/shm_ring.h
	@mkdir -p $(BINDIR)
	$(CC) $(CFLAGS) -c $< -o $@

# Shared library for the Python ctypes binding (python/shm_ring.py).
libshm_ring: $(BINDIR)/libshm_ring.so

$(BINDIR)/libshm_ring.so: src/shm_ring.c include/shm_ring.h
	@mkdir -p $(BINDIR)
	$(CC) $(CFLAGS) -fPIC -shared $< -o $@ -lrt

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

# ---- broker-layer benchmark (-O2 for realistic numbers) --------------------

$(BINDIR)/bench_broker: tests/bench_broker.cpp $(CLIENT_DEPS) $(SERVER_DEPS) $(BINDIR)/shm_ring.o
	@mkdir -p $(BINDIR)
	$(CXX) $(CXXFLAGS) -O2 $< $(BINDIR)/shm_ring.o -o $@ -lrt

bench_broker: $(BINDIR)/bench_broker
	@echo "\n========== bench_broker =========="; ./$(BINDIR)/bench_broker

# ---- examples --------------------------------------------------------------

$(ALL_EXAMPLES): $(BINDIR)/%: examples/%.cpp $(CLIENT_DEPS) $(SERVER_DEPS) examples/tick.hpp $(BINDIR)/shm_ring.o
	@mkdir -p $(BINDIR)
	$(CXX) $(CXXFLAGS) $< $(BINDIR)/shm_ring.o -o $@ -lrt

examples: $(ALL_EXAMPLES) $(BINDIR)/broker_daemon
	@for e in $(SELF_EXAMPLES); do \
		echo "\n========== $$e =========="; ./$$e; \
	done
	@echo "\n========== cross-process (producer / consumer) =========="
	@echo "built. run in three terminals:"
	@echo "  ./$(BINDIR)/broker_daemon"
	@echo "  ./$(BINDIR)/consumer"
	@echo "  ./$(BINDIR)/producer"

# ---- clean -----------------------------------------------------------------

clean:
	rm -rf $(BINDIR)
