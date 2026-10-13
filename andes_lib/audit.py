"""Audit and change-detection columns for lakehouse tables."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_audit_columns(df: DataFrame, batch_id: str, source_file: str) -> DataFrame:
    """Add ``ingested_at``, ``batch_id`` and ``source_file``: when, in which run and from where each row arrived."""
    return (
        df.withColumn("ingested_at", F.current_timestamp())
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("source_file", F.lit(source_file))
    )


def add_row_hash(df: DataFrame, cols: list[str]) -> DataFrame:
    """Add ``row_hash``: SHA-256 of ``cols`` cast to string and joined with ``||``.

    Used to detect real changes on MERGE. ``concat_ws`` skips nulls, so a null
    and a missing value hash the same; keep that in mind when choosing ``cols``.
    """
    return df.withColumn(
        "row_hash",
        F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in cols]), 256),
    )
