"""Tests for scripts/generate_erp_data.py.

Verifies that the clean (non-planted) portion of the generated journal
entries balances, and that exactly the documented planted errors exist:
3 unbalanced journals, 5 lines with a non-existent account, and 2
retroactive adjustments to a closed period with a recent
source_updated_at.
"""

import importlib.util
import math
from pathlib import Path

import pandas as pd
import pytest

_MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "generate_erp_data.py"
_SPEC = importlib.util.spec_from_file_location("generate_erp_data", _MODULE_PATH)
gen = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(gen)

BALANCE_TOLERANCE = 0.01
RETROACTIVE_RECENT_DAYS = 15


@pytest.fixture(scope="module")
def chart_of_accounts() -> pd.DataFrame:
    return gen.build_chart_of_accounts()


@pytest.fixture(scope="module")
def journal_entries() -> pd.DataFrame:
    return gen.generate_journal_entries()


@pytest.fixture(scope="module")
def journal_balances(journal_entries: pd.DataFrame) -> pd.Series:
    by_journal = journal_entries.groupby("journal_id")
    return by_journal["debit"].sum() - by_journal["credit"].sum()


def test_chart_of_accounts_has_about_40_accounts_with_hierarchy(chart_of_accounts):
    assert 35 <= len(chart_of_accounts) <= 45

    account_ids = set(chart_of_accounts["account_id"])
    parents = chart_of_accounts["parent_account_id"].dropna()
    assert len(parents) > 0
    assert parents.isin(account_ids).all()

    # headers are non-postable roots, leaves are postable children
    headers = chart_of_accounts[chart_of_accounts["parent_account_id"].isna()]
    leaves = chart_of_accounts[chart_of_accounts["parent_account_id"].notna()]
    assert not headers["is_postable"].any()
    assert leaves["is_postable"].all()


def test_cost_centers_has_6_entries():
    cost_centers = gen.build_cost_centers()
    assert len(cost_centers) == 6
    assert cost_centers["cost_center_id"].is_unique


def test_covers_24_months_for_both_entities(journal_entries):
    assert journal_entries["entity"].nunique() == 2
    assert set(journal_entries["entity"].unique()) == {"CR", "CO"}
    for entity in ("CR", "CO"):
        periods = journal_entries.loc[journal_entries["entity"] == entity, "period"]
        assert periods.nunique() == 24


def test_currency_matches_entity(journal_entries):
    mapping = journal_entries.drop_duplicates("entity").set_index("entity")["currency"]
    assert mapping["CR"] == "CRC"
    assert mapping["CO"] == "COP"
    assert journal_entries.groupby("entity")["currency"].nunique().eq(1).all()


def test_exactly_3_unbalanced_journals(journal_balances):
    unbalanced = journal_balances[journal_balances.abs() > BALANCE_TOLERANCE]
    assert len(unbalanced) == 3


def test_clean_journals_all_balance(journal_balances):
    unbalanced_ids = set(journal_balances[journal_balances.abs() > BALANCE_TOLERANCE].index)
    clean_balances = journal_balances.drop(index=unbalanced_ids)
    assert math.isclose(clean_balances.abs().sum(), 0.0, abs_tol=1e-6)


def test_exactly_5_lines_reference_a_nonexistent_account(journal_entries, chart_of_accounts):
    valid_ids = set(chart_of_accounts["account_id"])
    bogus_lines = journal_entries[~journal_entries["account_id"].isin(valid_ids)]
    assert len(bogus_lines) == 5


def test_bogus_account_lines_do_not_break_their_journal_balance(
    journal_entries, chart_of_accounts, journal_balances
):
    valid_ids = set(chart_of_accounts["account_id"])
    bogus_lines = journal_entries[~journal_entries["account_id"].isin(valid_ids)]
    bogus_journal_ids = set(bogus_lines["journal_id"])

    unbalanced_ids = set(journal_balances[journal_balances.abs() > BALANCE_TOLERANCE].index)
    # the bogus-account error is orthogonal to the unbalanced-journal error
    assert bogus_journal_ids.isdisjoint(unbalanced_ids)
    for journal_id in bogus_journal_ids:
        assert abs(journal_balances[journal_id]) <= BALANCE_TOLERANCE


def test_exactly_2_retroactive_adjustments_to_closed_periods(journal_entries):
    entries = journal_entries.copy()
    entries["posting_date"] = pd.to_datetime(entries["posting_date"]).dt.date
    entries["source_updated_at"] = pd.to_datetime(entries["source_updated_at"]).dt.date
    entries["is_closed"] = entries["period"].apply(gen.is_period_closed)
    entries["days_after_asof"] = entries["source_updated_at"].apply(
        lambda d: (gen.AS_OF_DATE - d).days
    )

    suspicious = entries[
        entries["is_closed"] & (entries["days_after_asof"] <= RETROACTIVE_RECENT_DAYS)
    ]
    assert suspicious["journal_id"].nunique() == 2


def test_main_writes_expected_csv_files(tmp_path):
    gen.main(output_dir=tmp_path)

    for filename in ("chart_of_accounts.csv", "cost_centers.csv", "journal_entries.csv"):
        path = tmp_path / filename
        assert path.exists()

    written = pd.read_csv(tmp_path / "journal_entries.csv")
    assert set(written.columns) == {
        "entry_id",
        "journal_id",
        "entity",
        "period",
        "posting_date",
        "account_id",
        "cost_center",
        "debit",
        "credit",
        "currency",
        "source_updated_at",
    }
    assert written["entry_id"].is_unique
