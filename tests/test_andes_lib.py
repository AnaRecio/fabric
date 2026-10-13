"""Tests for the andes_lib package (run on a local SparkSession)."""

from datetime import datetime
from decimal import Decimal

from andes_lib.audit import add_audit_columns, add_row_hash
from andes_lib.dedupe import keep_latest
from andes_lib.finance.checks import flag_late_adjustments, validate_gl

GL_SCHEMA = (
    "entry_id string, journal_id string, period string, account_id string, "
    "debit decimal(18,2), credit decimal(18,2)"
)


def _gl(spark, rows):
    return spark.createDataFrame(
        [(e, j, p, a, Decimal(d), Decimal(c)) for e, j, p, a, d, c in rows], GL_SCHEMA
    )


def test_add_audit_columns(spark):
    df = add_audit_columns(spark.createDataFrame([(1,)], "x int"), "b1", "Files/a.csv")
    row = df.collect()[0]
    assert (row.batch_id, row.source_file) == ("b1", "Files/a.csv")
    assert row.ingested_at is not None


def test_add_row_hash_detects_changes(spark):
    df = spark.createDataFrame([("a", 1), ("a", 1), ("a", 2)], "k string, v int")
    hashes = [r.row_hash for r in add_row_hash(df, ["k", "v"]).collect()]
    assert hashes[0] == hashes[1] != hashes[2]
    assert len(hashes[0]) == 64


def test_keep_latest(spark):
    df = spark.createDataFrame(
        [("e1", 1, "old"), ("e1", 3, "new"), ("e2", 2, "only")],
        "id string, ts int, v string",
    )
    out = {r.id: r.v for r in keep_latest(df, "id", "ts").collect()}
    assert out == {"e1": "new", "e2": "only"}


def test_validate_gl_reasons(spark):
    gl = _gl(
        spark,
        [
            ("1", "J1", "2026-01", "1000", "100", "0"),  # balanced, valid
            ("2", "J1", "2026-01", "2000", "0", "100"),
            ("3", "J2", "2026-01", "1000", "50", "0"),  # unbalanced journal
            ("4", "J2", "2026-01", "2000", "0", "40"),
            ("5", "J3", "2026-01", "9999", "10", "0"),  # unknown account
            ("6", "J3", "2026-01", "2000", "0", "10"),
            (None, "J4", "2026-01", "1000", "5", "5"),  # null key
        ],
    )
    accounts = spark.createDataFrame(
        [("1000",), ("2000",), ("2000",)], "account_id string"
    )

    good, bad = validate_gl(gl, accounts)

    assert good.columns == gl.columns
    assert bad.columns == [*gl.columns, "reason"]
    assert sorted(r.entry_id for r in good.collect()) == ["1", "2", "6"]
    reasons = {r.entry_id: r.reason for r in bad.collect()}
    assert reasons == {
        "3": "asiento_descuadrado",
        "4": "asiento_descuadrado",
        "5": "cuenta_inexistente",
        None: "llaves_nulas",
    }


def test_validate_gl_combines_reasons(spark):
    gl = _gl(spark, [("1", "J1", None, "9999", "10", "0")])
    accounts = spark.createDataFrame([("1000",)], "account_id string")
    _, bad = validate_gl(gl, accounts)
    assert (
        bad.collect()[0].reason
        == "llaves_nulas; cuenta_inexistente; asiento_descuadrado"
    )


def test_flag_late_adjustments(spark):
    df = spark.createDataFrame(
        [
            ("2026-01", datetime(2026, 2, 5)),  # within 10 days of 2026-01-31
            ("2026-01", datetime(2026, 2, 15)),  # late
            ("2026-02", None),
        ],
        "period string, source_updated_at timestamp",
    )
    flags = [r.is_late_adjustment for r in flag_late_adjustments(df, 10).collect()]
    assert flags == [False, True, None]
