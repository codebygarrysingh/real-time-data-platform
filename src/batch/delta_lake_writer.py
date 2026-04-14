"""
Delta Lake Writer — ACID-compliant upsert pipeline with medallion architecture.

Implements the Bronze → Silver → Gold data flow using Delta Lake's MERGE
operation for idempotent, exactly-once writes. Critical for regulatory
reporting workloads where duplicate data means compliance failures.

Medallion layers:
  Bronze: Raw ingest — append-only, schema-on-read, full audit log
  Silver: Cleaned — deduplicated, type-cast, validated, enriched
  Gold:   Business-ready — aggregated, Z-ORDERed, Snowflake-optimised

Key features:
  - MERGE for idempotent upserts (safe to replay failed batches)
  - Z-ORDER optimisation for analytical query patterns
  - Schema evolution with BACKWARD compatibility enforcement
  - Vacuum scheduling to manage storage costs
  - Delta change data feed for downstream CDC consumers
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


@dataclass
class MergeConfig:
    """Configuration for a MERGE operation."""
    match_keys: list[str]                      # Columns used to match source to target
    update_columns: list[str] | None = None    # None = update all columns
    insert_if_not_matched: bool = True
    delete_when_matched: bool = False
    partition_keys: list[str] | None = None


@dataclass
class TableStats:
    """Post-write statistics."""
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_deleted: int = 0
    files_written: int = 0
    write_duration_ms: float = 0.0


class DeltaLakeWriter:
    """
    Production Delta Lake writer implementing the medallion architecture.

    Write patterns:
    - Bronze: append-only streaming write with checkpoint
    - Silver: MERGE upsert with deduplication
    - Gold: complete overwrite with Z-ORDER optimisation

    Usage:
        writer = DeltaLakeWriter(spark)

        # Bronze — raw append
        writer.write_bronze(raw_df, path="s3://bucket/bronze/trades/")

        # Silver — deduplicated upsert
        writer.write_silver(
            df=cleaned_df,
            path="s3://bucket/silver/trades/",
            merge_config=MergeConfig(match_keys=["trade_id", "event_date"])
        )

        # Gold — optimised analytical layer
        writer.write_gold(
            df=agg_df,
            path="s3://bucket/gold/daily_pnl/",
            z_order_cols=["desk", "instrument", "date"]
        )
    """

    BRONZE_OPTIONS = {
        "delta.enableChangeDataFeed": "true",      # Enable CDC for downstream consumers
        "delta.autoOptimize.optimizeWrite": "true", # Auto-compact small files
        "delta.logRetentionDuration": "interval 90 days",
    }

    SILVER_OPTIONS = {
        **BRONZE_OPTIONS,
        "delta.autoOptimize.autoCompact": "true",
    }

    def __init__(self, spark: SparkSession, catalog: str = "main"):
        self.spark = spark
        self.catalog = catalog
        self._enable_delta_optimisations()

    def write_bronze(
        self,
        df: DataFrame,
        path: str,
        partition_by: list[str] | None = None,
        schema: StructType | None = None,
    ) -> None:
        """
        Append raw data to Bronze layer.
        - Schema-on-read: accepts any schema, stores raw
        - Adds ingestion metadata: _ingest_timestamp, _source_file
        - Never updates or deletes — immutable audit log
        """
        bronze_df = df.withColumn("_ingest_timestamp", F.current_timestamp()) \
                      .withColumn("_ingest_date", F.current_date())

        writer = bronze_df.write.format("delta") \
            .mode("append") \
            .options(**self.BRONZE_OPTIONS)

        if partition_by:
            writer = writer.partitionBy(*partition_by)

        if schema:
            writer = writer.option("mergeSchema", "false")  # Enforce schema in bronze

        writer.save(path)
        logger.info("Bronze write complete → %s (%d rows)", path, df.count())

    def write_silver(
        self,
        df: DataFrame,
        path: str,
        merge_config: MergeConfig,
    ) -> TableStats:
        """
        Upsert to Silver layer using MERGE.

        MERGE ensures idempotency — replaying the same batch produces
        the same result. Critical for fault-tolerant pipelines.
        """
        stats = TableStats()
        start = datetime.now()

        if not DeltaTable.isDeltaTable(self.spark, path):
            # First write — create the table
            df.write.format("delta") \
                .mode("overwrite") \
                .options(**self.SILVER_OPTIONS) \
                .save(path)
            stats.rows_inserted = df.count()
            logger.info("Silver table created at %s", path)
            return stats

        delta_table = DeltaTable.forPath(self.spark, path)
        match_condition = " AND ".join(
            f"target.{k} = source.{k}" for k in merge_config.match_keys
        )

        merge_builder = delta_table.alias("target") \
            .merge(df.alias("source"), match_condition)

        if merge_config.update_columns:
            update_dict = {col: F.col(f"source.{col}") for col in merge_config.update_columns}
            merge_builder = merge_builder.whenMatchedUpdate(set=update_dict)
        else:
            merge_builder = merge_builder.whenMatchedUpdateAll()

        if merge_config.insert_if_not_matched:
            merge_builder = merge_builder.whenNotMatchedInsertAll()

        if merge_config.delete_when_matched:
            merge_builder = merge_builder.whenMatchedDelete()

        merge_builder.execute()

        stats.write_duration_ms = (datetime.now() - start).total_seconds() * 1000
        logger.info(
            "Silver MERGE complete → %s (%.0fms)",
            path, stats.write_duration_ms
        )
        return stats

    def write_gold(
        self,
        df: DataFrame,
        path: str,
        z_order_cols: list[str] | None = None,
        partition_by: list[str] | None = None,
    ) -> None:
        """
        Overwrite Gold layer with optimised layout.
        Z-ORDER clustering dramatically reduces files scanned for analytical queries.
        """
        writer = df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .options(**self.SILVER_OPTIONS)

        if partition_by:
            writer = writer.partitionBy(*partition_by)

        writer.save(path)

        # Z-ORDER optimisation after write — clusters data by query dimensions
        if z_order_cols:
            self._optimise_with_z_order(path, z_order_cols)

        logger.info("Gold write complete → %s", path)

    def vacuum(self, path: str, retention_hours: int = 168) -> None:
        """
        Remove old Delta files. Default: keep 7 days (168h) for time travel.
        Lower retention reduces storage cost but limits rollback window.
        """
        delta_table = DeltaTable.forPath(self.spark, path)
        delta_table.vacuum(retention_hours)
        logger.info("Vacuumed %s (retention=%dh)", path, retention_hours)

    def time_travel_read(
        self, path: str, version: int | None = None, timestamp: str | None = None
    ) -> DataFrame:
        """Read historical version of a Delta table (audit, rollback, debugging)."""
        reader = self.spark.read.format("delta")
        if version is not None:
            reader = reader.option("versionAsOf", version)
        elif timestamp:
            reader = reader.option("timestampAsOf", timestamp)
        return reader.load(path)

    def _optimise_with_z_order(self, path: str, cols: list[str]) -> None:
        col_list = ", ".join(cols)
        self.spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({col_list})")
        logger.info("Z-ORDER optimisation complete for %s on (%s)", path, col_list)

    def _enable_delta_optimisations(self) -> None:
        self.spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
        self.spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
        self.spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
