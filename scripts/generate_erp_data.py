"""Generate synthetic multi-entity ERP general ledger data.

Deterministically (fixed random seed) generates 24 months of general
ledger data for two legal entities:

  - CR: Costa Rica, reporting currency CRC
  - CO: Colombia,   reporting currency COP

Outputs, written to ``data/erp/`` (gitignored, created on demand):

  - ``chart_of_accounts.csv``: ~40 accounts (Asset, Liability, Equity,
    Revenue, COGS, Opex-by-type) with a two-level parent/child
    hierarchy (non-postable header accounts + postable leaf accounts).
  - ``cost_centers.csv``: 6 cost centers.
  - ``journal_entries.csv``: GL lines with columns ``entry_id``,
    ``journal_id``, ``entity``, ``period``, ``posting_date``,
    ``account_id``, ``cost_center``, ``debit``, ``credit``,
    ``currency``, ``source_updated_at``.

Planted data-quality errors
----------------------------
The generator intentionally plants the following errors so that
downstream validation / tests have known-bad data to find. All other
journals are balanced, reference real accounts, and have a
``source_updated_at`` close to their ``posting_date``.

1. **3 unbalanced journals** -- one GL line's debit (or credit) is
   bumped by a fixed delta without a matching offset, so
   ``sum(debit) != sum(credit)`` for that ``journal_id``.
2. **5 lines with a non-existent account** -- the line's
   ``account_id`` is overwritten with a code (``9901``-``9905``) that
   does not appear in ``chart_of_accounts.csv``. The journal's debit
   /credit totals are left balanced, so this error is only detectable
   via account lookup, not via the balance check.
3. **2 retroactive adjustments to a closed period** -- a journal
   posted in an already-closed period has every line's
   ``source_updated_at`` rewritten to a date near ``AS_OF_DATE``,
   simulating someone editing closed books long after the fact. The
   journal stays balanced and uses real accounts.

Run as a script to (re)generate the CSVs::

    python scripts/generate_erp_data.py
"""

from __future__ import annotations

import calendar
import random
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from dateutil.relativedelta import relativedelta

SEED = 42

# Frozen "today" used to decide which periods are open/closed and how
# recent a retroactive edit looks. Kept independent of the real wall
# clock so output is fully reproducible.
AS_OF_DATE = date(2026, 10, 1)

NUM_MONTHS = 24
OPEN_PERIODS = 2  # most recent N periods are still open (not yet closed)

ENTITY_CURRENCY = {"CR": "CRC", "CO": "COP"}

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "erp"

COST_CENTERS = [
    ("CC01", "Corporate / HQ"),
    ("CC02", "Sales"),
    ("CC03", "Operations"),
    ("CC04", "Finance & Admin"),
    ("CC05", "Information Technology"),
    ("CC06", "Human Resources"),
]

# (account_id, account_name, account_type, parent_account_id, is_postable, account_subtype)
_HEADERS = [
    ("1000", "Assets", "Asset", None, False, ""),
    ("2000", "Liabilities", "Liability", None, False, ""),
    ("3000", "Equity", "Equity", None, False, ""),
    ("4000", "Revenue", "Revenue", None, False, ""),
    ("5000", "Cost of Goods Sold", "COGS", None, False, ""),
    ("6000", "Operating Expenses", "Opex", None, False, ""),
]

_ASSET_LEAVES = [
    ("1010", "Cash and Equivalents", "Asset", "1000", True, ""),
    ("1020", "Accounts Receivable", "Asset", "1000", True, ""),
    ("1030", "Inventory", "Asset", "1000", True, ""),
    ("1040", "Prepaid Expenses", "Asset", "1000", True, ""),
    ("1050", "Fixed Assets", "Asset", "1000", True, ""),
    ("1060", "Accumulated Depreciation", "Asset", "1000", True, ""),
]

_LIABILITY_LEAVES = [
    ("2010", "Accounts Payable", "Liability", "2000", True, ""),
    ("2020", "Accrued Liabilities", "Liability", "2000", True, ""),
    ("2030", "Taxes Payable", "Liability", "2000", True, ""),
    ("2040", "Short-term Debt", "Liability", "2000", True, ""),
]

_EQUITY_LEAVES = [
    ("3010", "Common Stock", "Equity", "3000", True, ""),
    ("3020", "Retained Earnings", "Equity", "3000", True, ""),
]

_REVENUE_LEAVES = [
    ("4010", "Product Revenue", "Revenue", "4000", True, ""),
    ("4020", "Service Revenue", "Revenue", "4000", True, ""),
    ("4030", "Other Revenue", "Revenue", "4000", True, ""),
]

_COGS_LEAVES = [
    ("5010", "Direct Materials", "COGS", "5000", True, ""),
    ("5020", "Direct Labor", "COGS", "5000", True, ""),
    ("5030", "Freight and Logistics", "COGS", "5000", True, ""),
]

_OPEX_LEAVES = [
    ("6110", "Salaries and Wages", "Opex", "6000", True, "Personnel"),
    ("6120", "Employee Benefits", "Opex", "6000", True, "Personnel"),
    ("6130", "Payroll Taxes", "Opex", "6000", True, "Personnel"),
    ("6210", "Rent", "Opex", "6000", True, "Facilities"),
    ("6220", "Utilities", "Opex", "6000", True, "Facilities"),
    ("6230", "Office Supplies", "Opex", "6000", True, "Facilities"),
    ("6310", "Advertising", "Opex", "6000", True, "Marketing"),
    ("6320", "Digital Marketing", "Opex", "6000", True, "Marketing"),
    ("6410", "Legal Fees", "Opex", "6000", True, "Professional Services"),
    ("6420", "Accounting and Consulting Fees", "Opex", "6000", True, "Professional Services"),
    ("6510", "Software Subscriptions", "Opex", "6000", True, "Technology"),
    ("6520", "Depreciation Expense - Equipment", "Opex", "6000", True, "Technology"),
    ("6610", "Travel", "Opex", "6000", True, "Travel and Other"),
    ("6620", "Meals and Entertainment", "Opex", "6000", True, "Travel and Other"),
    ("6630", "Bank Fees", "Opex", "6000", True, "Travel and Other"),
    ("6640", "Miscellaneous Expense", "Opex", "6000", True, "Travel and Other"),
]

_ALL_ACCOUNTS = (
    _HEADERS
    + _ASSET_LEAVES
    + _LIABILITY_LEAVES
    + _EQUITY_LEAVES
    + _REVENUE_LEAVES
    + _COGS_LEAVES
    + _OPEX_LEAVES
)

_REVENUE_IDS = [row[0] for row in _REVENUE_LEAVES]
_EXPENSE_IDS = [row[0] for row in _COGS_LEAVES + _OPEX_LEAVES]
_BALANCE_IDS = ["1010", "1020", "2010", "2020", "2030"]

JOURNALS_PER_PERIOD_RANGE = (12, 25)
LINE_COUNT_CHOICES = [2, 3, 4]
LINE_COUNT_WEIGHTS = [0.6, 0.3, 0.1]

AMOUNT_RANGE_BY_CURRENCY = {
    "CRC": (50_000, 3_000_000),
    "COP": (100_000, 15_000_000),
}

UNBALANCED_DELTA = 1_000
RETROACTIVE_RECENT_OFFSETS_DAYS = [3, 7]

# Targets for planted errors, expressed as (entity, period_index, journal_index[, line_index])
# so that regeneration with the fixed seed always lands on the same spots.
UNBALANCED_TARGETS = [
    ("CR", 2, 0),
    ("CO", 10, 1),
    ("CR", 20, 2),
]
BOGUS_ACCOUNT_TARGETS = [
    ("CR", 4, 0, 0),
    ("CR", 8, 1, 1),
    ("CO", 6, 0, 0),
    ("CO", 14, 2, 0),
    ("CR", 18, 0, 1),
]
BOGUS_ACCOUNT_IDS = ["9901", "9902", "9903", "9904", "9905"]
RETROACTIVE_TARGETS = [
    ("CR", 1, 3),
    ("CO", 0, 2),
]


def build_chart_of_accounts() -> pd.DataFrame:
    return pd.DataFrame(
        _ALL_ACCOUNTS,
        columns=["account_id", "account_name", "account_type", "parent_account_id", "is_postable", "account_subtype"],
    )


def build_cost_centers() -> pd.DataFrame:
    return pd.DataFrame(COST_CENTERS, columns=["cost_center_id", "cost_center_name"])


def build_periods() -> list[str]:
    start = AS_OF_DATE - relativedelta(months=NUM_MONTHS - 1)
    return [(start + relativedelta(months=i)).strftime("%Y-%m") for i in range(NUM_MONTHS)]


PERIODS = build_periods()


def is_period_closed(period: str) -> bool:
    return period not in PERIODS[-OPEN_PERIODS:]


def _random_day_in_period(period: str, rng: random.Random) -> date:
    year, month = (int(x) for x in period.split("-"))
    _, last_day = calendar.monthrange(year, month)
    return date(year, month, rng.randint(1, last_day))


def _build_journal(entity: str, period: str, seq: int, rng: random.Random) -> dict:
    currency = ENTITY_CURRENCY[entity]
    low, high = AMOUNT_RANGE_BY_CURRENCY[currency]

    mode = "revenue" if rng.random() < 0.3 else "expense"
    primary_pool = _REVENUE_IDS if mode == "revenue" else _EXPENSE_IDS

    n_lines = rng.choices(LINE_COUNT_CHOICES, weights=LINE_COUNT_WEIGHTS)[0]
    n_primary = min(n_lines - 1, len(primary_pool))
    primary_accounts = rng.sample(primary_pool, n_primary)
    balance_account = rng.choice(_BALANCE_IDS)

    posting_date = _random_day_in_period(period, rng)

    lines = []
    total = 0
    for account_id in primary_accounts:
        amount = round(rng.uniform(low, high), -2)
        total += amount
        debit, credit = (amount, 0) if mode == "expense" else (0, amount)
        lines.append(
            {
                "account_id": account_id,
                "cost_center": rng.choice(COST_CENTERS)[0],
                "debit": debit,
                "credit": credit,
                "posting_date": posting_date,
                "source_updated_at": posting_date + timedelta(days=rng.randint(0, 5)),
            }
        )

    debit, credit = (0, total) if mode == "expense" else (total, 0)
    lines.append(
        {
            "account_id": balance_account,
            "cost_center": rng.choice(COST_CENTERS)[0],
            "debit": debit,
            "credit": credit,
            "posting_date": posting_date,
            "source_updated_at": posting_date + timedelta(days=rng.randint(0, 5)),
        }
    )

    return {
        "journal_id": f"{entity}-{period}-{seq:04d}",
        "entity": entity,
        "period": period,
        "currency": currency,
        "lines": lines,
    }


def _plant_errors(journals_by_key: dict[tuple[str, int], list[dict]]) -> None:
    for entity, period_idx, journal_idx in UNBALANCED_TARGETS:
        line = journals_by_key[(entity, period_idx)][journal_idx]["lines"][0]
        if line["debit"] > 0:
            line["debit"] += UNBALANCED_DELTA
        else:
            line["credit"] += UNBALANCED_DELTA

    for account_id, (entity, period_idx, journal_idx, line_idx) in zip(
        BOGUS_ACCOUNT_IDS, BOGUS_ACCOUNT_TARGETS
    ):
        line = journals_by_key[(entity, period_idx)][journal_idx]["lines"][line_idx]
        line["account_id"] = account_id

    for offset_days, (entity, period_idx, journal_idx) in zip(
        RETROACTIVE_RECENT_OFFSETS_DAYS, RETROACTIVE_TARGETS
    ):
        assert is_period_closed(PERIODS[period_idx]), "retroactive target must be a closed period"
        journal = journals_by_key[(entity, period_idx)][journal_idx]
        recent_date = AS_OF_DATE - timedelta(days=offset_days)
        for line in journal["lines"]:
            line["source_updated_at"] = recent_date


def generate_journal_entries() -> pd.DataFrame:
    rng = random.Random(SEED)

    journals: list[dict] = []
    journals_by_key: dict[tuple[str, int], list[dict]] = {}

    for entity in ENTITY_CURRENCY:
        for period_idx, period in enumerate(PERIODS):
            n_journals = rng.randint(*JOURNALS_PER_PERIOD_RANGE)
            period_journals = [
                _build_journal(entity, period, seq, rng) for seq in range(1, n_journals + 1)
            ]
            journals.extend(period_journals)
            journals_by_key[(entity, period_idx)] = period_journals

    _plant_errors(journals_by_key)

    rows = []
    entry_id = 1
    for journal in journals:
        for line in journal["lines"]:
            rows.append(
                {
                    "entry_id": entry_id,
                    "journal_id": journal["journal_id"],
                    "entity": journal["entity"],
                    "period": journal["period"],
                    "posting_date": line["posting_date"].isoformat(),
                    "account_id": line["account_id"],
                    "cost_center": line["cost_center"],
                    "debit": line["debit"],
                    "credit": line["credit"],
                    "currency": journal["currency"],
                    "source_updated_at": line["source_updated_at"].isoformat(),
                }
            )
            entry_id += 1

    return pd.DataFrame(rows)


def main(output_dir: Path = DATA_DIR) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    coa = build_chart_of_accounts()
    cost_centers = build_cost_centers()
    journal_entries = generate_journal_entries()

    coa.to_csv(output_dir / "chart_of_accounts.csv", index=False)
    cost_centers.to_csv(output_dir / "cost_centers.csv", index=False)
    journal_entries.to_csv(output_dir / "journal_entries.csv", index=False)

    print(f"Wrote {len(coa)} accounts, {len(cost_centers)} cost centers, "
          f"{len(journal_entries)} GL lines to {output_dir}")


if __name__ == "__main__":
    main()
