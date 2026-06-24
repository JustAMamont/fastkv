#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# FastKV — install all dependencies needed to build and run integration tests
#
# Rules:
#   - Checks if tool already exists before installing
#   - Downloads latest stable versions via official APIs
#   - NO pipes into sudo (curl | sudo) — downloads to /tmp first
#
# Supported: Ubuntu/Debian, Fedora/RHEL, Alpine, macOS (Homebrew)
#
# Usage:
#   chmod +x scripts/install_deps.sh
#   ./scripts/install_deps.sh
# ─────────────────────────────────────────────────────────────────────────────

set -eo pipefail

# Safe increment — ((var++)) returns 1 when var==0, kills set -e
inc() { : $(( ${1}=${1}+1 )); }

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

section() { echo -e "\n${CYAN}━━━ $1 ━━━${NC}"; }
ok()     { echo -e "  ${GREEN}✓${NC} $1"; }
skip()   { echo -e "  ${YELLOW}→${NC} $1"; }
warn()   { echo -e "  ${YELLOW}Installing${NC} $1 ..."; }
fail()   { echo -e "  ${RED}✗${NC} $1"; }

installed=0
skipped=0

# ═══════════════════════════════════════════════════════════════════════════════
# Detect real user home even when run with sudo
# ═══════════════════════════════════════════════════════════════════════════════

if [ -n "${SUDO_USER:-}" ]; then
    REAL_HOME=$(getent passwd "$SUDO_USER" 2>/dev/null | cut -d: -f6 || eval echo "~$SUDO_USER")
else
    REAL_HOME="$HOME"
fi

# ═══════════════════════════════════════════════════════════════════════════════
# Smart command detection: PATH + common user-local directories
# ═══════════════════════════════════════════════════════════════════════════════

has_cmd() {
    command -v "$1" &>/dev/null && return 0
    for d in "$REAL_HOME/.cargo/bin" "$REAL_HOME/.local/bin" \
             "/usr/local/bin" "/snap/bin"; do
        [ -x "$d/$1" ] && return 0
    done
    return 1
}

# ═══════════════════════════════════════════════════════════════════════════════
# Package manager
# ═══════════════════════════════════════════════════════════════════════════════

detect_pkg_manager() {
    if command -v apt-get &>/dev/null; then echo "apt"
    elif command -v dnf &>/dev/null; then echo "dnf"
    elif command -v yum &>/dev/null; then echo "yum"
    elif command -v apk &>/dev/null; then echo "apk"
    elif command -v brew &>/dev/null; then echo "brew"
    else echo "unknown"
    fi
}

pkg_install() {
    local pm="$1"; shift
    case "$pm" in
        apt)   sudo apt-get update -qq 2>/dev/null && sudo apt-get install -y -qq "$@" 2>&1 | tail -1 ;;
        dnf)   sudo dnf install -y "$@" 2>&1 | tail -1 ;;
        yum)   sudo yum install -y "$@" 2>&1 | tail -1 ;;
        apk)   sudo apk add "$@" 2>&1 | tail -1 ;;
        brew)  brew install "$@" 2>&1 | tail -1 ;;
        *)     fail "Unknown package manager"; return 1 ;;
    esac
}

# Extend PATH so has_cmd() and version checks work
export PATH="${REAL_HOME}/.cargo/bin:${REAL_HOME}/.local/bin:/usr/local/bin:/snap/bin:${PATH:-}"

PKG="$(detect_pkg_manager)"

# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"
[ "$ARCH" = "x86_64" ] && ARCH="amd64"
[ "$ARCH" = "aarch64" ] && ARCH="arm64"

# Download to temp file (no pipe to sudo!)
dl() {
    local url="$1" dest="$2"
    curl -fL --progress-bar "$url" -o "$dest"
}

# ═══════════════════════════════════════════════════════════════════════════════

echo "╔═══════════════════════════════════════════════════════╗"
echo "║  FastKV — Dependency Installer                        ║"
echo "║  Package manager: ${PKG}                              ║"
echo "║  User home: ${REAL_HOME}                              ║"
echo "║  OS/Arch: ${OS}/${ARCH}                               ║"
echo "╚═══════════════════════════════════════════════════════╝"

# ═══════════════════════════════════════════════════════════════════════════════
# 1. Rust (cargo) — needed to build the server
# ═══════════════════════════════════════════════════════════════════════════════

section "Rust / Cargo"

if has_cmd rustc; then
    skip "Rust already installed: $(rustc --version 2>&1)"
    inc skipped
else
    warn "Rust (latest stable via rustup)"
    RUSTUP_TMP="/tmp/rustup-init.sh"
    dl "https://sh.rustup.rs" "$RUSTUP_TMP"
    export RUSTUP_HOME="${REAL_HOME}/.rustup"
    export CARGO_HOME="${REAL_HOME}/.cargo"
    sh "$RUSTUP_TMP" -y --default-toolchain stable --no-modify-path 2>&1 | tail -3
    rm -f "$RUSTUP_TMP"
    [ -n "${SUDO_USER:-}" ] && sudo chown -R "$SUDO_USER:$SUDO_USER" "$RUSTUP_HOME" "$CARGO_HOME" 2>/dev/null || true
    export PATH="${CARGO_HOME}/bin:${PATH:-}"
    ok "Rust installed: $(rustc --version 2>&1)"
    inc installed
fi

# ═══════════════════════════════════════════════════════════════════════════════
# 3. Python 3 + pip — needed for Python client tests
# ═══════════════════════════════════════════════════════════════════════════════

section "Python 3"

if has_cmd python3; then
    skip "Python3 already installed: $(python3 --version 2>&1)"
    inc skipped
else
    warn "Python 3"
    pkg_install "$PKG" python3 python3-pip || true
    ok "Python3 installed: $(python3 --version 2>&1)"
    inc installed
fi

# pip
if has_cmd pip3 2>/dev/null || python3 -m pip --version &>/dev/null; then
    skip "pip already installed"
else
    warn "pip"
    case "$PKG" in
        apt)   pkg_install "$PKG" python3-pip || true ;;
        apk)   pkg_install "$PKG" py3-pip || true ;;
        *)     pip_url="https://bootstrap.pypa.io/get-pip.py"
               dl "$pip_url" "/tmp/get-pip.py"
               python3 /tmp/get-pip.py --user 2>&1 | tail -2
               rm -f /tmp/get-pip.py ;;
    esac
    ok "pip installed"
    inc installed
fi

# ═══════════════════════════════════════════════════════════════════════════════
# 4. Netcat (optional — for server health check)
# ═══════════════════════════════════════════════════════════════════════════════

section "Netcat (optional)"

if has_cmd nc; then
    skip "nc already installed"
else
    warn "netcat"
    case "$PKG" in
        apt)     pkg_install "$PKG" netcat-openbsd ;;
        dnf|yum) pkg_install "$PKG" nmap-ncat ;;
        apk)     pkg_install "$PKG" netcat-openbsd ;;
        brew)    skip "usually pre-installed on macOS" ;;
        *)       fail "Install manually" ;;
    esac
    ok "nc installed"
    inc installed
fi

# ═══════════════════════════════════════════════════════════════════════════════
# Summary
# ═══════════════════════════════════════════════════════════════════════════════

echo ""
echo "═══════════════════════════════════════════════════"
echo "  ${BOLD}Installation complete${NC}"
echo "  Installed: ${GREEN}${installed}${NC}   Skipped: ${YELLOW}${skipped}${NC}"
echo "═══════════════════════════════════════════════════"

echo ""
echo -e "  ${CYAN}Tool versions:${NC}"
has_cmd rustc   && echo -e "    rustc:   $(rustc --version 2>&1)"                    || echo -e "    rustc:   ${RED}not found${NC}"
has_cmd cargo   && echo -e "    cargo:   $(cargo --version 2>&1)"                    || echo -e "    cargo:   ${RED}not found${NC}"
has_cmd python3 && echo -e "    python3: $(python3 --version 2>&1)"                  || echo -e "    python3: ${RED}not found${NC}"
has_cmd pip3    && echo -e "    pip3:    $(pip3 --version 2>&1 | cut -d' ' -f1-2)"   || echo -e "    pip3:    ${RED}not found${NC}"
has_cmd nc      && echo -e "    nc:      installed"                                  || echo -e "    nc:      ${YELLOW}not found (optional)${NC}"

echo ""
echo -e "  ${GREEN}Next steps:${NC}"
echo "    1. Build the server:  cargo build --release"
echo "    2. Start the server:  cargo run --release"
echo "    3. Run all tests:     ./scripts/setup_and_test.sh"
