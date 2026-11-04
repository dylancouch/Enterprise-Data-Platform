"""Shared Spark/Delta mechanics used by the synthetic domain examples."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def create_spark(app_name: str = "enterprise-data-platform") -> SparkSession:
    project_root = Path(__file__).resolve().parent
    builder = SparkSession.builder.appName(app_name)
    if "DATABRICKS_RUNTIME_VERSION" in __import__("os").environ:
        return builder.getOrCreate()
    from delta import configure_spark_with_delta_pip

    builder = (builder.master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.jars.ivy", str(project_root / ".cache" / "ivy"))
        .config("spark.local.dir", str(project_root / ".cache" / "spark-tmp"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    return configure_spark_with_delta_pip(builder).getOrCreate()


def write_delta(frame: DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(path))


def read_delta(spark: SparkSession, path: Path) -> DataFrame:
    return spark.read.format("delta").load(str(path))


def merge_raw(spark: SparkSession, incoming: DataFrame | None, path: Path, retry_keys: list[str]) -> tuple[DataFrame | None, int]:
    """Retain distinct raw envelopes across file replays and return duplicate count."""
    if path.exists():
        previous = read_delta(spark, path)
        incoming = previous if incoming is None else previous.unionByName(incoming, allowMissingColumns=True)
    if incoming is None:
        return None, 0
    before = incoming.count()
    merged = incoming.dropDuplicates(retry_keys)
    duplicates = before - merged.count()
    write_delta(merged, path)
    return merged, duplicates


def latest_revision(frame: DataFrame, keys: list[str], revision: str, updated_at: str, tie_break: str | None = None) -> DataFrame:
    order = [F.col(revision).cast("long").desc(), F.col(updated_at).desc()]
    if tie_break is not None:
        order.append(F.col(tie_break).asc())
    window = Window.partitionBy(*keys).orderBy(*order)
    return frame.withColumn("_rank", F.row_number().over(window)).filter("_rank = 1").drop("_rank")


def freshness_status(file_count: int, latest_update: datetime | None, assessment_time: datetime, stale_hours: int = 72) -> str:
    """Assess demo source presence and update age; this is not a production SLA."""
    if file_count == 0:
        return "MISSING_EXPECTED_FILE"
    if latest_update is None:
        return "MISSING_OR_INVALID_SOURCE_UPDATED_AT"
    if latest_update.tzinfo is None:
        latest_update = latest_update.replace(tzinfo=UTC)
    if (assessment_time - latest_update).total_seconds() > stale_hours * 3600:
        return f"STALE_SOURCE_UPDATE_OVER_{stale_hours}H"
    return "RECEIVED"


def publish_generation(pointer_path: Path, generation: str, run_id: str) -> dict[str, object]:
    """Atomically switch a local consumer pointer after all products are staged."""
    pointer = {"generation": generation, "run_id": run_id, "published_at_utc": datetime.now(UTC).isoformat(), "synthetic": True}
    pointer_path.parent.mkdir(parents=True, exist_ok=True)
    temp = pointer_path.with_suffix(".tmp")
    temp.write_text(json.dumps(pointer, indent=2) + "\n", encoding="utf-8")
    temp.replace(pointer_path)
    return pointer
