#!/bin/sh
# chappe installer — the daemon, the cross-device link, and the C++ library.
#
#   curl -sSL https://raw.githubusercontent.com/lolosioann/chappe/main/scripts/install.sh | sh
#
# Installs chappe_daemon and chappe_link, the C++ headers, libshm_ring.so, a
# pkg-config file and a CMake package config, then registers a systemd unit so
# the daemon comes up at boot.
#
# The Python client is not here: `pip install chappe` handles it, and the wheel
# builds its own copy of the ring.
#
# Knobs (all optional):
#   PREFIX=/usr/local        where to install
#   CHAPPE_REF=v3.0.0        version to build; defaults to the latest release
#   CHAPPE_USER=someone      uid the daemon runs as; see the note below
#   CHAPPE_SOCKET=/tmp/chappe.sock   listen address
#   SYSTEMD_DIR=/etc/systemd/system  where the unit goes
#   --no-systemd             install the binaries, register nothing
set -eu

REPO=lolosioann/chappe
PREFIX=${PREFIX:-/usr/local}
SOCKET=${CHAPPE_SOCKET:-/tmp/chappe.sock}
SYSTEMD_DIR=${SYSTEMD_DIR:-/etc/systemd/system}
WANT_SYSTEMD=yes
[ "${1:-}" = "--no-systemd" ] && WANT_SYSTEMD=no

say() { printf '%s\n' "$*"; }
die() { printf 'install.sh: %s\n' "$*" >&2; exit 1; }

# ---- preflight -------------------------------------------------------------
# Fail here with something actionable rather than half-installing and dying in
# the middle of a build.
[ "$(uname -s)" = Linux ] || die "chappe is Linux-only (it uses POSIX shm and SO_PEERCRED); found $(uname -s)"
for tool in curl tar make; do
  command -v "$tool" >/dev/null 2>&1 || die "need $tool"
done
command -v g++ >/dev/null 2>&1 || command -v c++ >/dev/null 2>&1 || die "need a C++17 compiler (g++ or c++)"

# sudo only where it is actually needed, so a --prefix inside $HOME needs none.
SUDO=
if [ ! -w "$(dirname "$PREFIX")" ] || { [ -e "$PREFIX" ] && [ ! -w "$PREFIX" ]; }; then
  command -v sudo >/dev/null 2>&1 || die "$PREFIX is not writable and sudo is not installed"
  SUDO=sudo
fi

# The daemon's socket is 0600 and gated on SO_PEERCRED, so it has to run as the
# same uid as the nodes that connect to it. Under sudo the interesting user is
# the one who called sudo, not root.
RUN_USER=${CHAPPE_USER:-${SUDO_USER:-$(id -un)}}
id "$RUN_USER" >/dev/null 2>&1 || die "no such user: $RUN_USER"

# ---- fetch -----------------------------------------------------------------
if [ -z "${CHAPPE_REF:-}" ]; then
  CHAPPE_REF=$(curl -sSL "https://api.github.com/repos/$REPO/releases/latest" |
               sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p' | head -1)
  # Deliberately not falling back to main: installing an unreleased tree from a
  # transient network error is exactly the surprise an installer must not spring.
  [ -n "$CHAPPE_REF" ] || die "could not resolve the latest release; set CHAPPE_REF=vX.Y.Z"
fi

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT INT TERM

say "chappe $CHAPPE_REF -> $PREFIX"
# -f so a 404 is a failed download, not an HTML error page piped into tar.
curl -fsSL -o "$TMP/src.tar.gz" \
  "https://github.com/$REPO/archive/refs/tags/$CHAPPE_REF.tar.gz" ||
  die "could not download $CHAPPE_REF — check that version exists"
tar xzf "$TMP/src.tar.gz" -C "$TMP" || die "could not unpack $CHAPPE_REF"
SRC=$(find "$TMP" -maxdepth 1 -type d -name 'chappe-*' | head -1)
[ -n "$SRC" ] || die "unexpected archive layout"

# ---- build and install -----------------------------------------------------
say "building..."
make -C "$SRC" -s daemon link libshm_ring
$SUDO make -C "$SRC" -s install "PREFIX=$PREFIX"

# ---- systemd ---------------------------------------------------------------
if [ "$WANT_SYSTEMD" = no ]; then
  say ""
  say "installed. skipped systemd (--no-systemd); start it yourself with:"
  say "  $PREFIX/bin/chappe_daemon $SOCKET"
  exit 0
fi

UNIT=$SYSTEMD_DIR/chappe.service
$SUDO mkdir -p "$SYSTEMD_DIR"
$SUDO tee "$UNIT" >/dev/null <<EOF
[Unit]
Description=chappe message broker
Documentation=https://github.com/$REPO
After=network.target

[Service]
Type=simple
ExecStart=$PREFIX/bin/chappe_daemon $SOCKET
User=$RUN_USER
Restart=always
RestartSec=1
# PrivateTmp would give the daemon a /tmp of its own, so the socket it created
# would be invisible to every client. The socket IS the interface here.
PrivateTmp=no

[Install]
WantedBy=multi-user.target
EOF

if [ "$SYSTEMD_DIR" = /etc/systemd/system ] && command -v systemctl >/dev/null 2>&1; then
  $SUDO systemctl daemon-reload
  $SUDO systemctl enable --now chappe.service
  STATE=$(systemctl is-active chappe.service || true)
  say ""
  say "chappe.service is $STATE and enabled at boot"
else
  say ""
  say "wrote $UNIT (not registered: no systemctl, or a non-standard SYSTEMD_DIR)"
fi

say ""
say "  daemon:  $PREFIX/bin/chappe_daemon on $SOCKET"
say "  link:    $PREFIX/bin/chappe_link"
say "  C++:     g++ app.cpp \$(pkg-config --cflags --libs chappe)"
say "           or find_package(chappe) in CMake"
say "  Python:  pip install chappe"
say ""
say "  running as user '$RUN_USER' — the socket is 0600 and checked with"
say "  SO_PEERCRED, so nodes must run as that user too."
