"""ctypes binding to the C shm ring (src/shm_ring.c), built as libshm_ring.so.

The ring's single-producer/multi-consumer handshake uses C11 seq_cst atomics
that Python can't replicate safely, so this calls the real C code rather than
reimplementing it. Same .c as the C++ side => identical shm layout, so a Python
node and a C++ node can share the same ring. Build:  make libshm_ring
"""
import ctypes
import glob
import os

_LIB = None


def _lib_path():
    """Locate the ring library: $CHAPPE_LIB, the copy a wheel drops beside this
    module, then the in-tree build, then the `make install` locations."""
    env = os.environ.get("CHAPPE_LIB")
    if env:
        return env
    here = os.path.dirname(os.path.abspath(__file__))
    # A wheel builds src/shm_ring.c as an extension, so the name carries the
    # interpreter's ABI tag (_shm_ring.cpython-313-x86_64-linux-gnu.so) rather
    # than being a plain libshm_ring.so.
    packaged = glob.glob(os.path.join(here, "_shm_ring*.so"))
    root = os.path.dirname(os.path.dirname(here))  # repo root, for an in-tree build
    for c in (packaged + [os.path.join(root, "bin", "libshm_ring.so"),
                          "/usr/local/lib/libshm_ring.so",
                          "/usr/lib/libshm_ring.so"]):
        if os.path.exists(c):
            return c
    return os.path.join(root, "bin", "libshm_ring.so")  # report in-tree path


def _load():
    global _LIB
    if _LIB is not None:
        return _LIB
    path = _lib_path()
    if not os.path.exists(path):
        raise RuntimeError(f"missing libshm_ring.so — set $CHAPPE_LIB or run "
                           f"`make libshm_ring` (looked for {path})")
    lib = ctypes.CDLL(path)
    P, I32, U32, SZ, CP = (ctypes.c_void_p, ctypes.c_int32, ctypes.c_uint32,
                           ctypes.c_size_t, ctypes.c_char_p)
    for fn, res, args in [
        ("shm_ring_create", P, [CP, SZ, U32]),
        ("shm_ring_attach", P, [CP]),
        ("shm_ring_acquire_slot", I32, [P]),
        ("shm_ring_publish_slot", None, [P, I32]),
        ("shm_ring_retain_latest", I32, [P]),
        ("shm_ring_release_slot", None, [P, I32]),
        ("shm_ring_slot_data", P, [P, I32]),
        ("shm_ring_slot_size", SZ, [P]),
        ("shm_ring_destroy", None, [P]),
    ]:
        f = getattr(lib, fn)
        f.restype = res
        f.argtypes = args
    _LIB = lib
    return lib


class Ring:
    """One process's handle to a ring. Producer via create(), consumer via
    attach() — same as the C++ SharedMemoryRing."""

    def __init__(self, handle):
        self._lib = _load()
        self._ring = handle

    @classmethod
    def create(cls, name, slot_size, num_slots):
        lib = _load()
        h = lib.shm_ring_create(name.encode(), slot_size, num_slots)
        if not h:
            raise RuntimeError(f"shm_ring_create failed for {name!r}")
        return cls(h)

    @classmethod
    def attach(cls, name):
        lib = _load()
        h = lib.shm_ring_attach(name.encode())
        if not h:
            raise RuntimeError(f"shm_ring_attach failed for {name!r}")
        return cls(h)

    @property
    def slot_size(self):
        return self._lib.shm_ring_slot_size(self._ring)

    def write(self, data):
        """Producer: copy `data` into a fresh slot and publish. Returns False if
        every slot is held by a consumer (frame dropped)."""
        size = self._lib.shm_ring_slot_size(self._ring)
        if len(data) > size:  # check before acquiring so we never leak a slot
            raise ValueError(f"frame {len(data)}B exceeds slot {size}B")
        idx = self._lib.shm_ring_acquire_slot(self._ring)
        if idx < 0:
            return False
        addr = self._lib.shm_ring_slot_data(self._ring, idx)
        ctypes.memmove(addr, data, len(data))  # bulk copy; slice-assign is O(n) in Python
        self._lib.shm_ring_publish_slot(self._ring, idx)
        return True

    def retain_latest(self):
        """Consumer: retain the newest frame. Returns (idx, memoryview) — a
        zero-copy view into the slot — or None if nothing is ready. Call
        release(idx) when done; don't use the view after that (the producer may
        reclaim the slot). Copy it out if you need to keep it."""
        idx = self._lib.shm_ring_retain_latest(self._ring)
        if idx < 0:
            return None
        size = self._lib.shm_ring_slot_size(self._ring)
        addr = self._lib.shm_ring_slot_data(self._ring, idx)
        return idx, memoryview((ctypes.c_char * size).from_address(addr))

    def release(self, idx):
        self._lib.shm_ring_release_slot(self._ring, idx)

    def destroy(self):
        if self._ring:
            self._lib.shm_ring_destroy(self._ring)
            self._ring = None
