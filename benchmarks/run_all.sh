#!/usr/bin/env bash
# run_all.sh — Master benchmark runner for the CDC pipeline research project.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
CONNECTOR_JSON="${ROOT_DIR}/debezium/postgres-connector.json"

KAFKA_CONNECT_URL="${KAFKA_CONNECT_URL:-http://localhost:8083}"
CONNECTOR_NAME="postgres-source-connector"

# ─── Colours ──────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
info()    { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*" >&2; }

# ─── Prerequisite checks ──────────────────────────────────────────────────────
info "Checking that all Docker Compose services are running …"

required_services=(postgres-source postgres-target zookeeper kafka kafka-connect redis fastapi)
for svc in "${required_services[@]}"; do
    status=$(docker inspect --format='{{.State.Health.Status}}' "$svc" 2>/dev/null || echo "missing")
    if [[ "$status" != "healthy" ]]; then
        error "Service '$svc' is not healthy (status: $status). Run ./scripts/setup.sh first."
        exit 1
    fi
done
info "All services healthy."

# ─── Register Debezium connector ──────────────────────────────────────────────
info "Registering Debezium connector …"

http_status=$(curl -s -o /dev/null -w "%{http_code}" \
    "${KAFKA_CONNECT_URL}/connectors/${CONNECTOR_NAME}")

if [[ "$http_status" == "200" ]]; then
    info "Connector '${CONNECTOR_NAME}' is already registered."
else
    # Substitute env vars into the connector config before posting
    tmp_config=$(mktemp /tmp/connector_XXXXXX.json)
    SOURCE_DB_HOST="${SOURCE_DB_HOST:-postgres-source}"
    SOURCE_DB_USER="${SOURCE_DB_USER:-postgres}"
    SOURCE_DB_PASSWORD="${SOURCE_DB_PASSWORD:-sourcepass}"

    python3 - <<EOF
import json, os, sys

with open("${CONNECTOR_JSON}") as f:
    cfg = json.load(f)

cfg["config"]["database.hostname"] = os.environ.get("SOURCE_DB_HOST", "postgres-source")
cfg["config"]["database.user"]     = os.environ.get("SOURCE_DB_USER", "postgres")
cfg["config"]["database.password"] = os.environ.get("SOURCE_DB_PASSWORD", "sourcepass")

with open("${tmp_config}", "w") as f:
    json.dump(cfg, f, indent=2)
EOF

    curl -sf -X POST \
        -H "Content-Type: application/json" \
        -d @"${tmp_config}" \
        "${KAFKA_CONNECT_URL}/connectors"

    rm -f "${tmp_config}"
    info "Connector registered."
fi

# ─── Wait for initial snapshot ────────────────────────────────────────────────
info "Waiting 30 s for Debezium to complete the initial snapshot …"
sleep 30

# ─── Run benchmarks ───────────────────────────────────────────────────────────
cd "${ROOT_DIR}"

info "Step 1/4  — Generating 100 000-record dataset …"
python3 benchmarks/generate_data.py --volume 100000

info "Step 2/4  — Running CDC pipeline benchmark …"
python3 benchmarks/benchmark_cdc.py

info "Step 3/4  — Running batch ETL baseline benchmark …"
python3 benchmarks/benchmark_batch_etl.py

info "Step 4/4  — Running FastAPI response-time benchmark …"
python3 benchmarks/measure_api.py

echo ""
echo -e "${GREEN}════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  All benchmarks complete. Results in benchmarks/results/${NC}"
echo -e "${GREEN}════════════════════════════════════════════════════════${NC}"
