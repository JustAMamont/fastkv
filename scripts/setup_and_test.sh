#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# FastKV — run integration tests for all client libraries
#
# Usage:
#   chmod +x scripts/setup_and_test.sh
#   ./scripts/setup_and_test.sh              # defaults: localhost:8379
#   ./scripts/setup_and_test.sh 127.0.0.1 6379
# ─────────────────────────────────────────────────────────────────────────────

set -eo pipefail

FASTKV_HOST="${1:-localhost}"
FASTKV_PORT="${2:-8379}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CLIENTS_DIR="$PROJECT_DIR/clients"

# Extend PATH so tools installed by install_deps.sh are found
# (Go in /usr/local/go/bin, Rust in ~/.cargo/bin, etc.)
export PATH="/usr/local/go/bin:${HOME}/.cargo/bin:${HOME}/.local/bin:/usr/local/bin:${PATH:-}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

total_passed=0
total_failed=0

inc() { : $(( ${1}=${1}+1 )); }

ok()     { echo -e "  ${GREEN}✓${NC} $1"; inc total_passed; }
fail()   { echo -e "  ${RED}✗${NC} $1 — $2"; inc total_failed; }
section(){ echo -e "\n${CYAN}━━━ $1 ━━━${NC}"; }

# Run a command, capture result — NEVER kills the script on failure
run_test() {
    local label="$1"; shift
    echo -e "  ${YELLOW}Running $label ...${NC}"
    if "$@" 2>&1; then
        ok "$label"
    else
        fail "$label" "see output above"
    fi
}

# ═══════════════════════════════════════════════════════════════════════════════
# 0. Check that FastKV server is reachable
# ═══════════════════════════════════════════════════════════════════════════════

section "Prerequisite check"

if command -v nc &>/dev/null; then
    if nc -z "$FASTKV_HOST" "$FASTKV_PORT" 2>/dev/null; then
        ok "FastKV server reachable at ${FASTKV_HOST}:${FASTKV_PORT}"
    else
        fail "FastKV server" "not reachable at ${FASTKV_HOST}:${FASTKV_PORT}"
        echo -e "\n${YELLOW}Start the server first:${NC}"
        echo -e "  cd $PROJECT_DIR && cargo run --release -- server ${FASTKV_PORT}"
        exit 1
    fi
else
    echo -e "  ${YELLOW}nc not available — skipping server check${NC}"
fi

# ═══════════════════════════════════════════════════════════════════════════════
# 1. Rust — unit tests
# ═══════════════════════════════════════════════════════════════════════════════

section "Rust (server)"

if command -v cargo &>/dev/null; then
    run_test "cargo test" bash -c "cd '$PROJECT_DIR' && timeout 120 cargo test 2>&1"
else
    echo -e "  ${YELLOW}cargo not found — skipping${NC}"
fi

# ═══════════════════════════════════════════════════════════════════════════════
# 2. Python client (sync)
# ═══════════════════════════════════════════════════════════════════════════════

section "Python client (sync)"

if command -v python3 &>/dev/null; then
    if [ -f "$CLIENTS_DIR/python/tests/test_integration.py" ]; then
        export FASTKV_HOST="$FASTKV_HOST"
        export FASTKV_PORT="$FASTKV_PORT"
        run_test "Python sync integration tests" \
            python3 "$CLIENTS_DIR/python/tests/test_integration.py"
    else
        fail "Python tests" "file not found: $CLIENTS_DIR/python/tests/test_integration.py"
    fi
else
    echo -e "  ${YELLOW}python3 not found — skipping${NC}"
fi

# 2b. Python client (async)
# ═══════════════════════════════════════════════════════════════════════════════

section "Python client (async)"

if command -v python3 &>/dev/null; then
    if [ -f "$CLIENTS_DIR/python/tests/test_async_integration.py" ]; then
        export FASTKV_HOST="$FASTKV_HOST"
        export FASTKV_PORT="$FASTKV_PORT"
        run_test "Python async integration tests" \
            python3 "$CLIENTS_DIR/python/tests/test_async_integration.py"
    else
        fail "Python async tests" "file not found: $CLIENTS_DIR/python/tests/test_async_integration.py"
    fi
else
    echo -e "  ${YELLOW}python3 not found — skipping${NC}"
fi

# ═══════════════════════════════════════════════════════════════════════════════
# 3. Go client
# ═══════════════════════════════════════════════════════════════════════════════

section "Go client"

GO_DIR="$CLIENTS_DIR/go/fastkv"

if command -v go &>/dev/null; then
    if [ -f "$GO_DIR/integration_test.go" ]; then
        export FASTKV_ADDR="${FASTKV_HOST}:${FASTKV_PORT}"
        run_test "Go integration tests" \
            bash -c "cd '$GO_DIR' && go test -v -count=1 -timeout 30s ./... 2>&1"
    else
        fail "Go tests" "file not found: $GO_DIR/integration_test.go"
    fi
else
    echo -e "  ${YELLOW}go not found — skipping${NC}"
fi

# ═══════════════════════════════════════════════════════════════════════════════
# 4. Java client
# ═══════════════════════════════════════════════════════════════════════════════

section "Java client"

JAVA_DIR="$CLIENTS_DIR/java"

if command -v javac &>/dev/null; then
    if [ -f "$JAVA_DIR/src/IntegrationTest.java" ]; then
        BUILD_DIR="$JAVA_DIR/target/test-classes"
        rm -rf "$BUILD_DIR"
        mkdir -p "$BUILD_DIR"

        echo -e "  ${YELLOW}Compiling Java ...${NC}"
        if (find "$JAVA_DIR/src" -name "*.java" | xargs javac -d "$BUILD_DIR" 2>&1); then
            ok "Java compiled"
            run_test "Java integration tests" \
                java -Dfastkv.host="$FASTKV_HOST" \
                     -Dfastkv.port="$FASTKV_PORT" \
                     -cp "$BUILD_DIR" \
                     com.fastkv.client.IntegrationTest
        else
            fail "Java compile" "see errors above"
        fi
    else
        fail "Java tests" "file not found: $JAVA_DIR/src/IntegrationTest.java"
    fi
else
    echo -e "  ${YELLOW}javac not found — skipping${NC}"
fi

# 4b. Java client (reactive)
# ═══════════════════════════════════════════════════════════════════════════════

section "Java client (reactive)"

if command -v javac &>/dev/null; then
    if [ -f "$JAVA_DIR/src/IntegrationTestReactive.java" ]; then
        run_test "Java reactive integration tests" \
            java -Dfastkv.host="$FASTKV_HOST" \
                 -Dfastkv.port="$FASTKV_PORT" \
                 -cp "$BUILD_DIR" \
                 com.fastkv.client.IntegrationTestReactive
    else
        fail "Java reactive tests" "file not found: $JAVA_DIR/src/IntegrationTestReactive.java"
    fi
else
    echo -e "  ${YELLOW}javac not found — skipping${NC}"
fi

# ═══════════════════════════════════════════════════════════════════════════════
# 5. Node.js client
# ═══════════════════════════════════════════════════════════════════════════════

section "Node.js client"

NODE_DIR="$CLIENTS_DIR/node/fastkv"

if command -v node &>/dev/null; then
    if [ -f "$NODE_DIR/tests/test_integration.js" ]; then
        run_test "Node.js integration tests" \
            node "$NODE_DIR/tests/test_integration.js" "$FASTKV_HOST" "$FASTKV_PORT"
    else
        fail "Node.js tests" "file not found: $NODE_DIR/tests/test_integration.js"
    fi
else
    echo -e "  ${YELLOW}node not found — skipping${NC}"
fi

# ═══════════════════════════════════════════════════════════════════════════════
# 6. Rust client (async)
# ═══════════════════════════════════════════════════════════════════════════════

section "Rust client"

RUST_CLIENT_DIR="$CLIENTS_DIR/rust"

if command -v cargo &>/dev/null; then
    if [ -f "$RUST_CLIENT_DIR/Cargo.toml" ]; then
        export FASTKV_HOST="$FASTKV_HOST"
        export FASTKV_PORT="$FASTKV_PORT"
        run_test "Rust client integration tests" \
            bash -c "cd '$RUST_CLIENT_DIR' && cargo test -- --test-threads=1 2>&1"
    else
        fail "Rust client tests" "file not found: $RUST_CLIENT_DIR/Cargo.toml"
    fi
else
    echo -e "  ${YELLOW}cargo not found — skipping${NC}"
fi

# ═══════════════════════════════════════════════════════════════════════════════
# Summary
# ═══════════════════════════════════════════════════════════════════════════════

echo ""
echo "═══════════════════════════════════════════════════"
echo "  SETUP & TEST COMPLETE"
echo "  Passed: ${GREEN}${total_passed}${NC}   Failed: ${RED}${total_failed}${NC}"
echo "═══════════════════════════════════════════════════"

if [ "$total_failed" -gt 0 ]; then
    exit 1
fi
