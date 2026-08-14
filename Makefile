CXX      := g++
CXXFLAGS := -std=c++17 -pthread -Wall -Wextra -Iinclude
CC       := cc
CFLAGS   := -std=c11 -pthread -Wall -Wextra -Iinclude
BINDIR   := bin

# install layout (override PREFIX / DESTDIR as usual):
#   $(PREFIX)/bin/chappe_daemon
#   $(PREFIX)/lib/libshm_ring.so
#   $(PREFIX)/include/chappe/*.hpp,*.h  (+ ipc/)
#   $(PREFIX)/lib/chappe/python/*.py
PREFIX ?= /usr/local
DESTDIR ?=
# Kept in step with chappe::VERSION and python's __version__ by test_chappe.py.
VERSION := 2.0.0
PUB_HEADERS := include/chappe.hpp include/node.hpp include/server.hpp \
               include/threadpool.hpp include/link.hpp include/shm_ring.h

# Client (Node) headers. node.hpp pulls in the shm ring (frame API) and the
# wire layer, so anything using Node depends on these and links shm_ring.o.
CLIENT_DEPS := include/node.hpp include/chappe.hpp include/threadpool.hpp \
               include/ipc/shm_ring.hpp include/ipc/frame_handle.hpp \
               include/ipc/transport.hpp include/shm_ring.h
# Daemon (Server) headers — no shm, sockets only.
SERVER_DEPS := include/server.hpp include/ipc/transport.hpp include/chappe.hpp
# Cross-device link — transport only, no client and no shm.
LINK_DEPS   := include/link.hpp include/ipc/transport.hpp include/chappe.hpp

# ---- targets ---------------------------------------------------------------

ALL_TESTS := $(BINDIR)/test_threadpool $(BINDIR)/test_node \
             $(BINDIR)/test_frame_ipc $(BINDIR)/test_transport \
             $(BINDIR)/test_link
# Self-contained demos (embedded daemon) — built and run by `make examples`.
SELF_EXAMPLES := $(BINDIR)/basic $(BINDIR)/keyvalue $(BINDIR)/frames
# Cross-process demo — built only; run by hand against a chappe_daemon.
IPC_EXAMPLES  := $(BINDIR)/producer $(BINDIR)/consumer
ALL_EXAMPLES  := $(SELF_EXAMPLES) $(IPC_EXAMPLES)

.PHONY: all test examples daemon link libshm_ring install uninstall clean \
        test_threadpool test_node test_frame_ipc test_transport test_link \
        test_shm_ring stress_shm_ring bench_shm_ring

all: test examples daemon link libshm_ring

# ---- broker daemon ---------------------------------------------------------

daemon: $(BINDIR)/chappe_daemon

$(BINDIR)/chappe_daemon: src/chappe_daemon.cpp $(SERVER_DEPS)
	@mkdir -p $(BINDIR)
	$(CXX) $(CXXFLAGS) $< -o $@

# ---- cross-device link -----------------------------------------------------

link: $(BINDIR)/chappe_link

$(BINDIR)/chappe_link: src/chappe_link.cpp $(LINK_DEPS)
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

$(BINDIR)/test_link: tests/test_link.cpp $(CLIENT_DEPS) $(SERVER_DEPS) $(LINK_DEPS) include/test.hpp $(BINDIR)/shm_ring.o
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

test_link: $(BINDIR)/test_link
	@echo "\n========== test_link =========="; ./$(BINDIR)/test_link

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
	echo "\n========== test_link =========="; \
	./$(BINDIR)/test_link;         status5=$$?; \
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

$(BINDIR)/bench_chappe: tests/bench_chappe.cpp $(CLIENT_DEPS) $(SERVER_DEPS) $(BINDIR)/shm_ring.o
	@mkdir -p $(BINDIR)
	$(CXX) $(CXXFLAGS) -O2 $< $(BINDIR)/shm_ring.o -o $@ -lrt

bench_chappe: $(BINDIR)/bench_chappe
	@echo "\n========== bench_chappe =========="; ./$(BINDIR)/bench_chappe

# ---- examples --------------------------------------------------------------

$(ALL_EXAMPLES): $(BINDIR)/%: examples/%.cpp $(CLIENT_DEPS) $(SERVER_DEPS) examples/tick.hpp $(BINDIR)/shm_ring.o
	@mkdir -p $(BINDIR)
	$(CXX) $(CXXFLAGS) $< $(BINDIR)/shm_ring.o -o $@ -lrt

examples: $(ALL_EXAMPLES) $(BINDIR)/chappe_daemon
	@for e in $(SELF_EXAMPLES); do \
		echo "\n========== $$e =========="; ./$$e; \
	done
	@echo "\n========== cross-process (producer / consumer) =========="
	@echo "built. run in three terminals:"
	@echo "  ./$(BINDIR)/chappe_daemon"
	@echo "  ./$(BINDIR)/consumer"
	@echo "  ./$(BINDIR)/producer"

# ---- install ---------------------------------------------------------------

# pkg-config and CMake describe the same thing: headers under include/chappe,
# link libshm_ring + threads + rt. They are generated here in the recipe rather
# than as file targets, because their content depends on $(PREFIX) — a variable
# make can't take a dependency on, so a file target would go stale the moment
# you installed to a second prefix. @PREFIX@ is the install prefix and never
# DESTDIR: a staged package must say where it will finally live.
install: $(BINDIR)/chappe_daemon $(BINDIR)/chappe_link $(BINDIR)/libshm_ring.so
	sed -e 's|@PREFIX@|$(PREFIX)|g' -e 's|@VERSION@|$(VERSION)|g' \
	    packaging/chappe.pc.in > $(BINDIR)/chappe.pc
	sed -e 's|@VERSION@|$(VERSION)|g' \
	    packaging/chappeConfig.cmake.in > $(BINDIR)/chappeConfig.cmake
	sed -e 's|@VERSION@|$(VERSION)|g' \
	    packaging/chappeConfigVersion.cmake.in > $(BINDIR)/chappeConfigVersion.cmake
	install -d $(DESTDIR)$(PREFIX)/bin $(DESTDIR)$(PREFIX)/lib \
	           $(DESTDIR)$(PREFIX)/include/chappe/ipc \
	           $(DESTDIR)$(PREFIX)/lib/chappe/python/chappe \
	           $(DESTDIR)$(PREFIX)/lib/pkgconfig \
	           $(DESTDIR)$(PREFIX)/lib/cmake/chappe
	install -m 755 $(BINDIR)/chappe_daemon  $(DESTDIR)$(PREFIX)/bin/
	install -m 755 $(BINDIR)/chappe_link    $(DESTDIR)$(PREFIX)/bin/
	install -m 644 $(BINDIR)/libshm_ring.so $(DESTDIR)$(PREFIX)/lib/
	install -m 644 $(PUB_HEADERS)           $(DESTDIR)$(PREFIX)/include/chappe/
	install -m 644 include/ipc/*.hpp        $(DESTDIR)$(PREFIX)/include/chappe/ipc/
	install -m 644 python/chappe/*.py       $(DESTDIR)$(PREFIX)/lib/chappe/python/chappe/
	install -m 644 $(BINDIR)/chappe.pc      $(DESTDIR)$(PREFIX)/lib/pkgconfig/
	install -m 644 $(BINDIR)/chappeConfig.cmake $(BINDIR)/chappeConfigVersion.cmake \
	                                        $(DESTDIR)$(PREFIX)/lib/cmake/chappe/
	@echo "installed chappe $(VERSION) to $(DESTDIR)$(PREFIX)"
	@echo "  C++:    pkg-config --cflags --libs chappe   (or find_package(chappe))"
	@echo "  Python: export PYTHONPATH=$(PREFIX)/lib/chappe/python   (or pip install chappe)"
	@echo "  daemon: $(PREFIX)/bin/chappe_daemon"

uninstall:
	rm -f  $(DESTDIR)$(PREFIX)/bin/chappe_daemon
	rm -f  $(DESTDIR)$(PREFIX)/bin/chappe_link
	rm -f  $(DESTDIR)$(PREFIX)/lib/libshm_ring.so
	rm -f  $(DESTDIR)$(PREFIX)/lib/pkgconfig/chappe.pc
	rm -rf $(DESTDIR)$(PREFIX)/lib/cmake/chappe
	rm -rf $(DESTDIR)$(PREFIX)/include/chappe
	rm -rf $(DESTDIR)$(PREFIX)/lib/chappe
	@echo "uninstalled from $(DESTDIR)$(PREFIX)"

# ---- clean -----------------------------------------------------------------

clean:
	rm -rf $(BINDIR)
