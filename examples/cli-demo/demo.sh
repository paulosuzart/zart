#!/usr/bin/env bash
# CLI Demo — self-contained orchestration script
#
# This script:
# 1. Generates a unique execution ID
# 2. Starts the Rust durable execution in background
# 3. Runs CLI admin commands against it
# 4. Cleans up the background process on exit
#
# Usage: just example-cli-demo
#   or:  DATABASE_URL=postgres://... bash examples/cli-demo/demo.sh

set -euo pipefail

# ── Colors ────────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

DATABASE_URL="${DATABASE_URL:-postgres://zart:zart@localhost:5432/zart}"
EXECUTION_ID="cli-demo-$(uuidgen | tr '[:upper:]' '[:lower:]' | cut -d'-' -f1-3)"
export EXECUTION_ID

# Silence all cargo/rust logging — only our echo output should appear
export RUST_LOG=off

ZART="cargo run -q -p zart-cli -- --database-url ${DATABASE_URL}"

# ── Cleanup on exit ───────────────────────────────────────────────────────────
RUST_PID=""
cleanup() {
    if [ -n "$RUST_PID" ] && kill -0 "$RUST_PID" 2>/dev/null; then
        echo ""
        echo -e "${YELLOW}Stopping background Rust process (PID: $RUST_PID)...${NC}"
        kill "$RUST_PID" 2>/dev/null || true
        wait "$RUST_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

# ── Header ────────────────────────────────────────────────────────────────────
echo ""
echo -e "${BLUE}╔══════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║        Zart CLI Admin Commands — Interactive Demo       ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${CYAN}Execution ID: ${EXECUTION_ID}${NC}"
echo -e "${CYAN}Database:     ${DATABASE_URL}${NC}"
echo ""

# ── Start Rust process in background ─────────────────────────────────────────
echo -e "${GREEN}Starting durable execution in background...${NC}"
DATABASE_URL="${DATABASE_URL}" EXECUTION_ID="${EXECUTION_ID}" \
    cargo run -p example-cli-demo &
RUST_PID=$!
echo -e "${GREEN}✓ Rust process started (PID: $RUST_PID)${NC}"
echo ""

# ── Wait for execution to initialize in DB ────────────────────────────────────
echo -e "${GREEN}Waiting for execution to initialize...${NC}"
for i in {1..15}; do
    if cargo run -q -p zart-cli -- --database-url "${DATABASE_URL}" status "${EXECUTION_ID}" >/dev/null 2>&1; then
        echo -e "${GREEN}✓ Execution initialized ($i seconds)${NC}"
        break
    fi
    if [ $i -eq 15 ]; then
        echo -e "${RED}✗ ERROR: Execution did not initialize within 15 seconds${NC}"
        exit 1
    fi
    sleep 1
done
echo ""

# ── Helper: run a CLI command (fails hard on error) ──────────────────────────
run_cli() {
    local description="$1"
    shift

    echo -e "${YELLOW}▶ ${description}${NC}"
    echo -e "${BLUE}  $ ${*}${NC}"
    echo ""
    "$@" 2>&1
    echo ""
    echo -e "${BLUE}─────────────────────────────────────────────────────────${NC}"
    echo ""
}

# ── 1. Status ─────────────────────────────────────────────────────────────────
run_cli "Check execution status" \
    ${ZART} status "${EXECUTION_ID}"

# ── 2. Pause ──────────────────────────────────────────────────────────────────
echo -e "${GREEN}Creating pause rule...${NC}"
run_cli "Create pause rule for this execution" \
    ${ZART} pause --execution-id "${EXECUTION_ID}" --triggered-by demo-script

# ── 3. List pause rules ───────────────────────────────────────────────────────
run_cli "List active pause rules" \
    ${ZART} pause-list

# Brief pause
echo -e "${GREEN}Waiting 3 seconds...${NC}"
sleep 3

# ── 4. Resume ─────────────────────────────────────────────────────────────────
echo -e "${GREEN}Resuming execution...${NC}"
run_cli "Resume by soft-deleting pause rules" \
    ${ZART} resume --execution-id "${EXECUTION_ID}" --triggered-by demo-script

# ── 5. Verify rules deleted ───────────────────────────────────────────────────
run_cli "Verify pause rules are deleted" \
    ${ZART} pause-list

# Wait for execution to progress
echo -e "${GREEN}Execution resumed. Waiting 8 seconds...${NC}"
sleep 8

# ── 6. Status after resume ────────────────────────────────────────────────────
run_cli "Check status after resume" \
    ${ZART} status "${EXECUTION_ID}"

# ── 7. Restart ────────────────────────────────────────────────────────────────
echo -e "${GREEN}Demonstrating full restart...${NC}"
run_cli "Restart execution with new payload" \
    ${ZART} restart "${EXECUTION_ID}" --payload '{"fail_step":false}' --triggered-by demo-script

# ── 8. List runs ──────────────────────────────────────────────────────────────
run_cli "List all runs (original + restart)" \
    ${ZART} runs "${EXECUTION_ID}"

# Wait for restarted execution
echo -e "${GREEN}Waiting 10 seconds for restarted execution...${NC}"
sleep 10

# ── 9. Final status ───────────────────────────────────────────────────────────
run_cli "Final status check" \
    ${ZART} status "${EXECUTION_ID}"

# ── 10. Final run history ─────────────────────────────────────────────────────
run_cli "Final run history" \
    ${ZART} runs "${EXECUTION_ID}"

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}╔══════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║              CLI Demo Commands Complete                  ║${NC}"
echo -e "${GREEN}╚══════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${BLUE}Commands demonstrated:${NC}"
echo "  • zart status         — Check execution status"
echo "  • zart pause          — Create pause rules"
echo "  • zart resume         — Soft-delete pause rules"
echo "  • zart pause-list     — List all pause rules"
echo "  • zart restart        — Full restart with history preservation"
echo "  • zart runs           — List run history"
echo ""
echo -e "${YELLOW}Background process will be cleaned up automatically.${NC}"
echo ""
