"""Deduplication helpers."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def keep_latest(df: DataFrame, key: str | list[str], order_col: str) -> DataFrame:
    """Keep one row per ``key``: the one with the highest ``order_col``.

    Ties on ``order_col`` are broken arbitrarily.
    """
    keys = [key] if isinstance(key, str) else key
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
