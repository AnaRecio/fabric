"""General-ledger validations: quarantine bad rows and flag late adjustments."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Reason codes written to the quarantine ``reason`` column (kept identical to
# the values already stored in quarantine_gl_entries).
REASON_NULL_KEYS = "llaves_nulas"
REASON_UNKNOWN_ACCOUNT = "cuenta_inexistente"
REASON_UNBALANCED = "asiento_descuadrado"

BALANCE_TOLERANCE = 0.01


def validate_gl(
    gl: DataFrame,
    valid_accounts: DataFrame,
    tolerance: float = BALANCE_TOLERANCE,
) -> tuple[DataFrame, DataFrame]:
    """Split GL lines into ``(good, quarantined)``.

    ``gl`` needs ``entry_id``, ``journal_id``, ``account_id``, ``period``,
    ``debit`` and ``credit``; ``valid_accounts`` needs ``account_id``.
    A line is quarantined if any of these hold (all that apply are listed in
    ``reason``, separated by ``"; "``):

    - ``llaves_nulas``: ``entry_id``, ``account_id`` or ``period`` is null.
    - ``cuenta_inexistente``: ``account_id`` is not in ``valid_accounts``.
    - ``asiento_descuadrado``: its journal's debits and credits differ by more
      than ``tolerance``; every line of that journal is quarantined.

    ``good`` keeps the input columns; ``quarantined`` adds ``reason``.
    """
    accounts = (
        valid_accounts.select("account_id")
        .distinct()
        .withColumn("_acct_ok", F.lit(True))
    )

    unbalanced = (
        gl.groupBy("journal_id")
        .agg((F.sum("debit") - F.sum("credit")).alias("_diff"))
        .filter(F.abs("_diff") > tolerance)
        .select("journal_id")
        .withColumn("_unbalanced", F.lit(True))
    )

    checked = (
        gl.join(accounts, "account_id", "left")
        .join(unbalanced, "journal_id", "left")
        .withColumn(
            "reason",
            F.concat_ws(
                "; ",
                F.when(
                    F.col("entry_id").isNull()
                    | F.col("account_id").isNull()
                    | F.col("period").isNull(),
                    F.lit(REASON_NULL_KEYS),
                ),
                F.when(F.col("_acct_ok").isNull(), F.lit(REASON_UNKNOWN_ACCOUNT)),
                F.when(F.col("_unbalanced"), F.lit(REASON_UNBALANCED)),
            ),
        )
        .drop("_acct_ok", "_unbalanced")
    )

    good = checked.filter(F.col("reason") == "").drop("reason").select(*gl.columns)
    quarantined = checked.filter(F.col("reason") != "").select(*gl.columns, "reason")
    return good, quarantined


def flag_late_adjustments(df: DataFrame, days: int) -> DataFrame:
    """Add ``is_late_adjustment``: ``source_updated_at`` is more than ``days`` after the end of ``period``.

    ``period`` is ``YYYY-MM``. This is not an accounting error, so it is a flag,
    not a quarantine reason.
    """
    period_end = F.last_day(F.to_date(F.concat(F.col("period"), F.lit("-01"))))
    return df.withColumn(
        "is_late_adjustment",
        F.col("source_updated_at") > F.date_add(period_end, days),
    )
