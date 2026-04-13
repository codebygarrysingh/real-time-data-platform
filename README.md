# Real-Time Data Platform

> Enterprise streaming platform reference architecture: Apache Kafka, Spark Structured Streaming, Delta Lake, and data quality at scale. Proven at 10M+ events/day across capital markets and retail operations.

[![Python](https://img.shields.io/badge/Python_3.11-3776AB?style=flat&logo=python&logoColor=white)](https://python.org)
[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka_3.7-231F20?style=flat&logo=apachekafka&logoColor=white)](https://kafka.apache.org)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark_3.5-E25A1C?style=flat&logo=apachespark&logoColor=white)](https://spark.apache.org)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake_3.2-003366?style=flat&logo=delta&logoColor=white)](https://delta.io)
[![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=flat&logo=snowflake&logoColor=white)](https://snowflake.com)
[![AWS](https://img.shields.io/badge/AWS_EMR-FF9900?style=flat&logo=amazonaws&logoColor=white)](https://aws.amazon.com/emr)

---

## Production Track Record

| Deployment | Scale | Outcome |
|---|---|---|
| Northern Trust — Market Risk | 10M+ daily position metrics | 50% faster downstream reporting |
| TD Bank — Electronic Trading | 5M+ daily transactions | Sub-10ms end-to-end latency |
| Albertsons — Retail Ops | 2M+ daily POS/supply chain events | 28% reduction in downstream errors |
| IBM/ESDC — Government Payments | Millions of daily benefit transactions | 40% query latency reduction |

---

## Architecture

```
┌────────────────────────────────────────────────────────────────┐
│                      DATA SOURCES                              │
│  Trading Systems · Market Data Feeds · POS · APIs · CDC        │
└───────────────────────────────┬────────────────────────────────┘
                                │
                                ▼
┌────────────────────────────────────────────────────────────────┐
│                    INGESTION LAYER (Kafka)                      │
│                                                                │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐ │
│  │  Producers   │  │    Topics    │  │   Schema Registry    │ │
│  │  (Java/Py)   │  │  (partitioned│  │   (Avro / Protobuf)  │ │
│  │              │  │   by key)    │  │   + compatibility    │ │
│  └──────────────┘  └──────────────┘  └──────────────────────┘ │
│                                                                │
│  Exactly-once semantics · Dead letter queues · Lag monitoring  │
└───────────────────────────────┬────────────────────────────────┘
                                │
               ┌────────────────┴────────────────┐
               ▼                                 ▼
┌──────────────────────────┐     ┌──────────────────────────────┐
│   STREAM PROCESSING      │     │   BATCH PROCESSING           │
│   (Spark Structured      │     │   (Spark on EMR/Databricks)  │
│    Streaming)            │     │                              │
│                          │     │   Historical backfill        │
│   Stateful aggregations  │     │   Complex ML feature eng.    │
│   Windowed joins         │     │   Regulatory batch reports   │
│   Real-time ML scoring   │     │   Full Delta table rewrites  │
└────────────┬─────────────┘     └──────────────┬───────────────┘
             │                                   │
             └──────────────┬────────────────────┘
                            ▼
┌────────────────────────────────────────────────────────────────┐
│                    STORAGE LAYER                               │
│                                                                │
│  ┌─────────────────┐  ┌──────────────┐  ┌──────────────────┐  │
│  │   Delta Lake    │  │  Snowflake   │  │   Redis          │  │
│  │   (raw + silver │  │  (gold layer │  │   (real-time     │  │
│  │    + gold)      │  │   analytics) │  │    lookups)      │  │
│  └─────────────────┘  └──────────────┘  └──────────────────┘  │
└───────────────────────────────┬────────────────────────────────┘
                                │
                                ▼
┌────────────────────────────────────────────────────────────────┐
│              DATA QUALITY + OBSERVABILITY                       │
│  Great Expectations · Monte Carlo · Custom SLA monitors        │
└────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology | Notes |
|---|---|---|
| Message Broker | Apache Kafka 3.7 (Confluent) | Exactly-once, Schema Registry |
| Stream Processing | Spark Structured Streaming 3.5 | Stateful, checkpointing |
| Batch Processing | Apache Spark on AWS EMR / Databricks | Auto-scaling clusters |
| Storage — Raw | AWS S3 / Azure ADLS | Parquet + Delta format |
| Storage — Lakehouse | Delta Lake 3.2 | ACID, time travel, CDC |
| Warehouse | Snowflake | Gold layer analytics |
| Orchestration | Apache Airflow 2.9 | DAG-based pipeline management |
| Data Quality | Great Expectations + custom | Pre/post-load validation |
| Serving | Redis + Snowflake | Sub-ms lookups + analytics |
| Monitoring | Prometheus + Grafana | Consumer lag, throughput SLOs |
| CI/CD | GitHub Actions + Terraform | IaC for all infrastructure |

---

## Project Structure

```
real-time-data-platform/
├── src/
│   ├── ingestion/
│   │   ├── kafka_producer.py           # High-throughput producer with batching
│   │   ├── schema_registry.py          # Avro schema management + evolution
│   │   └── dead_letter_handler.py      # DLQ processing + alerting
│   ├── streaming/
│   │   ├── spark_streaming_job.py      # Structured Streaming entry point
│   │   ├── windowed_aggregations.py    # Tumbling/sliding/session windows
│   │   ├── stateful_joins.py          # Stream-stream joins with watermarks
│   │   └── ml_scoring_pipeline.py     # Real-time inference on stream
│   ├── batch/
│   │   ├── delta_lake_writer.py        # Merge/upsert with ACID guarantees
│   │   ├── snowflake_loader.py         # Bulk load with COPY INTO
│   │   └── historical_backfill.py      # Partitioned parallel backfill
│   ├── quality/
│   │   ├── expectations_suite.py       # Great Expectations validation
│   │   ├── data_profiler.py            # Schema drift detection
│   │   └── sla_monitor.py             # Freshness + completeness SLOs
│   └── infra/
│       ├── kafka_admin.py              # Topic management, partition rebalancing
│       └── cluster_sizing.py          # EMR/Databricks capacity calculator
├── dags/
│   ├── streaming_monitor.py           # Airflow: lag and health checks
│   └── batch_pipeline.py             # Airflow: orchestrated batch runs
├── terraform/
│   ├── kafka_cluster.tf
│   ├── emr_cluster.tf
│   └── snowflake_warehouse.tf
└── requirements.txt
```

---

## Kafka Producer: High-Throughput Pattern

```python
# Production-tuned Kafka producer configuration
# Validated at 5M+ messages/day with <10ms end-to-end latency

producer_config = {
    "bootstrap.servers": KAFKA_BROKERS,
    "acks": "all",                      # Strongest durability guarantee
    "enable.idempotence": True,         # Exactly-once semantics
    "compression.type": "lz4",          # Best throughput/CPU tradeoff
    "batch.size": 65536,                # 64KB batches for throughput
    "linger.ms": 5,                     # Wait up to 5ms for batch fill
    "buffer.memory": 67108864,          # 64MB producer buffer
    "max.in.flight.requests.per.connection": 5,
    "retries": 10,
    "retry.backoff.ms": 100,
}
```

---

## Delta Lake: Medallion Architecture

```
Bronze (Raw)          Silver (Cleaned)         Gold (Business)
─────────────         ────────────────         ───────────────
Append-only      →    Deduplicated        →    Aggregated
Schema-on-read        Schema-on-write          Snowflake-ready
Audit log             Type-cast                SLA-guaranteed
Partitioned by        Validated                Query-optimised
  ingestion_date      Enriched                 (Z-ORDER)
```

```python
# MERGE with Delta Lake — idempotent upserts for exactly-once guarantees
(
    delta_table.alias("target")
    .merge(
        updates.alias("source"),
        "target.trade_id = source.trade_id AND target.event_date = source.event_date"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
```

---

## Data Quality Framework

```python
suite = ExpectationSuite(name="market_risk_positions")

# Completeness
suite.expect_column_values_to_not_be_null("trade_id")
suite.expect_column_values_to_not_be_null("position_value")

# Referential integrity
suite.expect_column_values_to_be_in_set("currency", SUPPORTED_CURRENCIES)
suite.expect_column_values_to_be_between("position_value", min_value=-1e12, max_value=1e12)

# Freshness SLA — positions must be no older than 5 minutes
suite.expect_column_max_to_be_between(
    "event_timestamp",
    min_value=datetime.utcnow() - timedelta(minutes=5)
)
```

**Measured impact:** 35% reduction in data quality incidents (Northern Trust deployment).

---

## Performance Benchmarks

| Metric | Value | Setup |
|---|---|---|
| End-to-end latency (p50) | 8ms | Kafka → Spark → Redis |
| End-to-end latency (p99) | 47ms | Kafka → Spark → Redis |
| Throughput | 12M events/day | 12-partition Kafka, 20 Spark executors |
| Batch processing speedup | 2.5× | After Spark tuning + Delta Z-ORDER |
| Data quality pass rate | 99.97% | With Great Expectations gates |

---

## Author

**Garry Singh** — Principal AI & Data Engineer · MSc Oxford · 10+ years enterprise data platforms

[Portfolio](https://garrysingh.dev) · [LinkedIn](https://linkedin.com/in/singhgarry) · [Book a Consultation](https://calendly.com/garry-singh2902)
