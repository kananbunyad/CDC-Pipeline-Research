# Real-Time CDC-Based Data Pipeline for Cloud Databases

> **Companion repository for the paper:**
> *"Real-Time CDC-Based Data Pipeline for Cloud Databases: Architecture, Implementation and Performance Evaluation in E-Gaming CRM Systems"*

This repository contains all source code, infrastructure configuration, and benchmark scripts required to reproduce the experiments described in the paper. The pipeline captures row-level changes from a PostgreSQL source database via Debezium/Kafka Connect, streams them through Apache Kafka, and materialises them in a DigitalOcean Managed PostgreSQL target — achieving sub-second end-to-end latency at production-grade data volumes.

---

## Prerequisites

| Tool | Minimum Version |
|------|----------------|
| Docker | 24.0 |
| Docker Compose | 2.20 |
| Python | 3.11 |
| Git | 2.40 |

Tested on Ubuntu 22.04 LTS and macOS 14 (Apple Silicon). At least **8 GB RAM** and **20 GB free disk** are recommended for the full benchmark suite.

---

## Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/your-org/cdc-pipeline-research.git
cd cdc-pipeline-research

# 2. Copy environment template
cp .env.example .env
# Edit .env and set TARGET_DB_HOST / SOURCE_DB_HOST if needed (defaults work locally)

# 3. Bootstrap the entire stack
chmod +x scripts/setup.sh scripts/teardown.sh benchmarks/run_all.sh
./scripts/setup.sh

# 4. Run all benchmarks (takes ~30–60 minutes for full suite)
./benchmarks/run_all.sh
```

Results land in `benchmarks/results/` as both JSON and CSV.

---

## Repository Structure

```
.
├── README.md                      # This file
├── docker-compose.yml             # Full stack: Postgres × 2, Kafka, Debezium, Redis, FastAPI, Airflow
├── .env.example                   # Environment variable template
├── requirements.txt               # Python dependencies
│
├── benchmarks/
│   ├── run_all.sh                 # Master benchmark runner
│   ├── generate_data.py           # Synthetic e-gaming CRM data generator
│   ├── benchmark_cdc.py           # CDC latency / throughput / consistency benchmark
│   ├── benchmark_batch_etl.py     # Batch ETL baseline benchmark
│   ├── measure_api.py             # FastAPI response-time benchmark (with/without cache)
│   └── results/                   # Output directory (JSON + CSV)
│
├── src/
│   ├── consumers/
│   │   └── kafka_consumer.py      # Micro-batch Kafka → Postgres consumer
│   ├── api/
│   │   ├── main.py                # FastAPI application with RFM analytics
│   │   └── Dockerfile             # Container image for the API service
│   ├── airflow_dags/
│   │   └── batch_etl_dag.py       # Airflow DAG: 1-minute batch ETL pipeline
│   └── db/
│       └── schema.sql             # Table definitions + indexes + publication
│
├── debezium/
│   └── postgres-connector.json    # Debezium PostgreSQL connector configuration
│
└── scripts/
    ├── setup.sh                   # One-shot environment bootstrap
    └── teardown.sh                # Stop and remove all containers + volumes
```

---

## Benchmark Replication

### 1. CDC Pipeline Benchmark

```bash
# Generate 100 000-record dataset (paper's primary evaluation point)
python benchmarks/generate_data.py --volume 100000

# Run CDC benchmark across all configured volumes
python benchmarks/benchmark_cdc.py
# Results → benchmarks/results/cdc_results.{json,csv}
```

### 2. Batch ETL Baseline

```bash
python benchmarks/benchmark_batch_etl.py
# Results → benchmarks/results/batch_results.{json,csv}
```

### 3. API Response-Time Benchmark

```bash
python benchmarks/measure_api.py
# Results → benchmarks/results/api_results.{json,csv}
```

### 4. Full Suite (all of the above)

```bash
./benchmarks/run_all.sh
```

---

## Expected Results

The table below reproduces **Table 3** from the paper (CDC pipeline, 100 000-record workload, 10 repetitions).

| Metric | CDC Pipeline | Batch ETL (1 min) | Batch ETL (5 min) |
|--------|-------------|-------------------|-------------------|
| Mean latency (ms) | **389** | 31 420 | 152 870 |
| Std deviation (ms) | 47 | 8 210 | 41 300 |
| p50 latency (ms) | 381 | 29 800 | 148 200 |
| p95 latency (ms) | 463 | 57 300 | 289 500 |
| p99 latency (ms) | 512 | 59 100 | 296 800 |
| Throughput (rec/s) | **2 301** | 4 800 | 4 820 |
| Data consistency (%) | **99.96** | 100.00 | 100.00 |

> Hardware: DigitalOcean Droplet 8 vCPU / 16 GB RAM, Frankfurt region.
> Actual numbers on different hardware will vary; relative ordering is stable.

---

## Citation

If you use this code or data in academic work, please cite:

```bibtex
@article{bunyadov2025cdc,
  title     = {Real-Time {CDC}-Based Data Pipeline for Cloud Databases:
               Architecture, Implementation and Performance Evaluation
               in E-Gaming {CRM} Systems},
  author    = {Bunyadov, Kanan},
  journal   = {Journal of Data Engineering},
  year      = {2025},
  volume    = {1},
  number    = {1},
  pages     = {1--20},
  doi       = {10.xxxx/jde.2025.001},
  url       = {https://github.com/your-org/cdc-pipeline-research}
}
```

---

## License

This project is released under the **MIT License**. See [LICENSE](LICENSE) for details.

© 2025 Kanan Bunyadov
