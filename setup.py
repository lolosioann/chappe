# Everything except the C ring is declared in pyproject.toml. This file exists
# only because there is no TOML table for ext_modules.
#
# chappe._shm_ring is NOT an importable extension module — it has no PyInit_ and
# will fail if you `import` it. Declaring it as one is a deliberate lever: it is
# what gets src/shm_ring.c compiled for the running interpreter and installed
# inside the package, where chappe/shm_ring.py finds it and ctypes.CDLL()s it by
# path. Same .c as the C++ side, so the shm layout stays identical and a Python
# node can share a ring with a C++ one.
from setuptools import Extension, setup

setup(
    ext_modules=[
        Extension(
            "chappe._shm_ring",
            sources=["src/shm_ring.c"],
            include_dirs=["include"],
            libraries=["rt"],
            extra_compile_args=["-std=c11", "-pthread"],
            extra_link_args=["-pthread"],
        )
    ]
)
