# Data Agent: andes_finance_agent

Versioned configuration for the Fabric Data Agent. When you change the agent in Fabric,
update this file in the same pull request and re-run the evaluation set (`eval_set.md`).

## 1. Data sources

Lakehouse `andes_lakehouse`, tables:

- `silver_gl_entries`: validated general-ledger lines (main fact table).
- `silver_dim_account`: chart of accounts.
- `silver_dim_cost_center`: cost centers.
- `quarantine_gl_entries`: rows rejected by data-quality checks (only for data-quality questions).

Do not add Bronze tables: they contain raw, unvalidated and duplicated data.

## 2. Agent instructions (paste into "Agent instructions")

```
You are a finance data assistant for Andes Outdoor S.A., a company with two legal entities:
CR (Costa Rica, currency CRC) and CO (Colombia, currency COP).

Data rules:
- Use silver_gl_entries as the source of truth for accounting figures. Never use quarantine
  data for financial figures.
- Amounts are in local currency. There is no FX conversion yet. Never add CR and CO amounts
  together. When a question covers both entities, answer per entity and state the currency.
- period is a text column in YYYY-MM format. "Last month" means the most recent period
  present in the data, not the calendar month.
- Revenue = SUM(credit - debit) on revenue accounts.
  Expenses = SUM(debit - credit) on expense accounts (COGS and operating expenses).
- Join silver_gl_entries.account_id to silver_dim_account.account_id to get account names
  and types. Join silver_gl_entries.cost_center to silver_dim_cost_center.cost_center_id to get
  cost center names.
- is_late_adjustment = true marks entries modified after their period closed. Mention it when
  a requested figure includes late adjustments.
- Questions about data quality, rejected rows or errors use quarantine_gl_entries and its
  reason column.

Answer style:
- Answer in the language of the question.
- Lead with the number, then a one-line explanation of how it was calculated.
- Format amounts with thousands separators and the currency code.
- If the question is ambiguous (for example, no entity or period), state the assumption you
  made.
- If the data cannot answer the question, say so. Never invent figures.
```

Adjust the account-type names in the revenue and expense rules to match the values in your
`silver_dim_account` table (check with `SELECT DISTINCT account_type FROM silver_dim_account`).

## 3. Data source instructions (paste into the lakehouse data source)

```
silver_gl_entries: one row per validated journal line. Key columns: entry_id (unique),
journal_id (groups balanced lines), entity (CR or CO), period (YYYY-MM), posting_date,
account_id, cost_center, debit, credit, currency, is_late_adjustment.
silver_dim_account: chart of accounts, one row per account_id.
silver_dim_cost_center: cost_center_id, cost_center_name.
quarantine_gl_entries: rejected rows with a reason column (cuenta_inexistente,
asiento_descuadrado, llaves_nulas) and run_id.
```

## 4. Example queries (add as "Example queries")

**Revenue by entity for the latest period**

```sql
SELECT g.entity, g.currency, SUM(g.credit - g.debit) AS revenue
FROM silver_gl_entries g
JOIN silver_dim_account a ON a.account_id = g.account_id
WHERE a.account_type = 'Revenue'
  AND g.period = (SELECT MAX(period) FROM silver_gl_entries)
GROUP BY g.entity, g.currency;
```

**Top 3 cost centers by expenses in Costa Rica, last 12 periods**

```sql
SELECT TOP 3 c.cost_center_name, SUM(g.debit - g.credit) AS expenses
FROM silver_gl_entries g
JOIN silver_dim_account a ON a.account_id = g.account_id
JOIN silver_dim_cost_center c ON c.cost_center_id = g.cost_center
WHERE g.entity = 'CR'
  AND a.account_type IN ('COGS', 'Operating Expense')
  AND g.period >= (SELECT FORMAT(DATEADD(month, -11, CAST(MAX(period) + '-01' AS date)), 'yyyy-MM')
                   FROM silver_gl_entries)
GROUP BY c.cost_center_name
ORDER BY expenses DESC;
```

**Late adjustments**

```sql
SELECT entity, period, journal_id, account_id, debit, credit, source_updated_at
FROM silver_gl_entries
WHERE is_late_adjustment = 1
ORDER BY source_updated_at DESC;
```

**Data-quality summary**

```sql
SELECT reason, COUNT(*) AS rows_rejected, COUNT(DISTINCT journal_id) AS journals
FROM quarantine_gl_entries
GROUP BY reason;
```

Replace `'Revenue'`, `'COGS'` and `'Operating Expense'` with the actual values in your
chart of accounts.
