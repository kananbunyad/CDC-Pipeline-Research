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
git clone https://github.com/kananbunyad/CDC-Pipeline-Research.git
cd CDC-Pipeline-Research

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

The table below reproduces **Table 2** from the paper (100,000-record workload, 10 repetitions).

### Latency (milliseconds)

| Volume | CDC Mean | CDC P95 | CDC P99 | Batch-1min Mean | Batch-5min Mean |
|--------|----------|---------|---------|-----------------|-----------------|
| 1K     | 187      | 256     | 312     | 61,200          | 301,200         |
| 5K     | 213      | 298     | 367     | 62,400          | 303,000         |
| 10K    | 248      | 351     | 428     | 64,800          | 306,000         |
| 50K    | 312      | 441     | 537     | 72,000          | 318,000         |
| 100K   | **389**  | **548** | **672** | **84,000**      | **336,000**     |
| 500K   | 523      | 738     | 891     | 126,000         | 420,000         |

### Throughput (records/second)

| Volume | CDC    | Batch ETL | Improvement |
|--------|--------|-----------|-------------|
| 1K     | 2,847  | 1,523     | 1.87x       |
| 5K     | 2,712  | 1,487     | 1.82x       |
| 10K    | 2,634  | 1,412     | 1.87x       |
| 50K    | 2,489  | 1,289     | 1.93x       |
| 100K   | **2,301** | **1,134** | **2.03x** |
| 500K   | 1,876  | 847       | 2.21x       |

> CDC achieves **215x** lower latency than Batch-1min and **864x** lower than Batch-5min at 100K records.
> Hardware: DigitalOcean Droplet 8 vCPU / 16 GB RAM, Frankfurt region.

---

## Citation

If you use this code or data in academic work, please cite:

```bibtex
@misc{bunyadov2025cdc,
  title  = {Real-Time {CDC}-Based Data Pipeline for Cloud Databases:
             Architecture, Implementation and Performance Evaluation
             in E-Gaming {CRM} Systems},
  author = {Bunyadov, Kanan},
  year   = {2025},
  note   = {Manuscript under review},
  url    = {https://github.com/kananbunyad/CDC-Pipeline-Research}
}
```

---

## License

This project is released under the **MIT License**. See [LICENSE](LICENSE) for details.

© 2025 Kanan Bunyadov
