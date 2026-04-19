#!/usr/bin/env bash
# setup.sh — Bootstrap the full CDC pipeline research environment.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT_DIR}"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }

# ─── Prerequisites ────────────────────────────────────────────────────────────
info "Checking prerequisites …"

command -v docker >/dev/null 2>&1 || { error "docker not found. Install Docker 24+."; exit 1; }
DOCKER_VERSION=$(docker --version | grep -oP '\d+\.\d+' | head -1 | cut -d. -f1)
if [[ "$DOCKER_VERSION" -lt 24 ]]; then
    warn "Docker version ${DOCKER_VERSION} detected; 24+ recommended."
fi

if docker compose version >/dev/null 2>&1; then
    COMPOSE_CMD="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE_CMD="docker-compose"
else
    error "docker-compose not found. Install Docker Compose 2.20+."
    exit 1
fi

PYTHON_CMD=""
for candidate in python3.11 python3 python; do
    if command -v "$candidate" >/dev/null 2>&1; then
        version=$("$candidate" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>/dev/null)
        major=$(echo "$version" | cut -d. -f1)
        minor=$(echo "$version" | cut -d. -f2)
        if [[ "$major" -ge 3 && "$minor" -ge 11 ]]; then
            PYTHON_CMD="$candidate"
            break
        fi
    fi
done

if [[ -z "$PYTHON_CMD" ]]; then
    error "Python 3.11+ not found. Install it before continuing."
    exit 1
fi
info "Using Python: $($PYTHON_CMD --version)"

# ─── Environment file ─────────────────────────────────────────────────────────
if [[ ! -f .env ]]; then
    info "Copying .env.example → .env"
    cp .env.example .env
    warn "Review .env and update credentials before running benchmarks."
else
    info ".env already exists — skipping copy."
fi

# ─── Python virtual environment ───────────────────────────────────────────────
if [[ ! -d .venv ]]; then
    info "Creating Python virtual environment in .venv …"
    "$PYTHON_CMD" -m venv .venv
fi

info "Installing Python dependencies …"
.venv/bin/pip install --upgrade pip --quiet
# Install without apache-airflow (heavy; installed separately if needed)
grep -v "^apache-airflow" requirements.txt > /tmp/reqs_noairflow.txt
.venv/bin/pip install --quiet -r /tmp/reqs_noairflow.txt

# ─── Start Docker Compose stack ───────────────────────────────────────────────
info "Starting Docker Compose stack …"
$COMPOSE_CMD up -d

# ─── Wait for services ────────────────────────────────────────────────────────
info "Waiting for services to become healthy …"

wait_healthy() {
    local container="$1"
    local timeout=120
    local elapsed=0
    while [[ $elapsed -lt $timeout ]]; do
        status=$(docker inspect --format='{{.State.Health.Status}}' "$container" 2>/dev/null || echo "missing")
        if [[ "$status" == "healthy" ]]; then
            info "${container} is healthy."
            return 0
        fi
        sleep 5
        elapsed=$((elapsed + 5))
    done
    error "${container} did not become healthy within ${timeout}s."
    return 1
}

for svc in postgres-source postgres-target zookeeper kafka kafka-connect redis fastapi; do
    wait_healthy "$svc"
done

# ─── Smoke test ───────────────────────────────────────────────────────────────
info "Running smoke test: inserting 1 000 records …"
source .env 2>/dev/null || true
.venv/bin/python benchmarks/generate_data.py --volume 1000

# ─── Done ─────────────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  Setup complete!${NC}"
echo -e "${GREEN}════════════════════════════════════════════════════════${NC}"
echo ""
echo "  Next steps:"
echo "    1. Review benchmarks/run_all.sh"
echo "    2. Run:  ./benchmarks/run_all.sh"
echo "    3. Results will appear in:  benchmarks/results/"
echo ""
echo "  Service URLs:"
echo "    FastAPI       →  http://localhost:8000/docs"
echo "    Airflow       →  http://localhost:8080  (admin / admin)"
echo "    Kafka Connect →  http://localhost:8083/connectors"
echo ""
