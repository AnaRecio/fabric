# Data Agent evaluation set

Run the ground-truth SQL in the lakehouse SQL analytics endpoint, write the expected answer,
then ask the agent each question and score it. Re-run the whole set after every change to the
agent's instructions, examples or data sources, and record the score in the history table.

Scoring: 1 = correct number and correct interpretation; 0.5 = right approach but wrong
filter, rounding or missing caveat; 0 = wrong or invented.

| # | Question | Tests |
| --- | --- | --- |
| 1 | ¿Cuántos ingresos tuvo Costa Rica en el último período? | Revenue sign, "last period" rule |
| 2 | What were total expenses in Colombia in 2025? | Expense sign, year filter |
| 3 | ¿Cuál es el ingreso total de la empresa el último mes? | Must refuse to sum CRC + COP |
| 4 | Top 3 cost centers by expenses in Costa Rica | Join to cost centers, ranking |
| 5 | ¿Cuántos asientos contables hay por entidad? | journal_id vs line count |
| 6 | Which entries are late adjustments? | Use of is_late_adjustment |
| 7 | ¿Cuántas filas se rechazaron por calidad de datos y por qué? | Uses quarantine, not Silver |
| 8 | Compare Sales cost-center expenses in CR between the last two periods | Period arithmetic |
| 9 | ¿Qué cuenta de gasto creció más en Colombia en el último trimestre? | Multi-step reasoning |
| 10 | What is our EBITDA? | Must say it cannot answer without the definition and accounts |

## Ground-truth SQL

Replace account-type values with the ones in `silver_dim_account`.

```sql
-- Q1
SELECT SUM(g.credit - g.debit) AS revenue_crc
FROM silver_gl_entries g JOIN silver_dim_account a ON a.account_id = g.account_id
WHERE g.entity = 'CR' AND a.account_type = 'Revenue'
  AND g.period = (SELECT MAX(period) FROM silver_gl_entries);

-- Q2
SELECT SUM(g.debit - g.credit) AS expenses_cop
FROM silver_gl_entries g JOIN silver_dim_account a ON a.account_id = g.account_id
WHERE g.entity = 'CO' AND a.account_type IN ('COGS', 'Operating Expense')
  AND g.period LIKE '2025-%';

-- Q3: expected answer is two figures (CRC and COP), never a single sum.

-- Q4
SELECT TOP 3 c.cost_center_name, SUM(g.debit - g.credit) AS expenses_crc
FROM silver_gl_entries g
JOIN silver_dim_account a ON a.account_id = g.account_id
JOIN silver_dim_cost_center c ON c.cost_center_id = g.cost_center
WHERE g.entity = 'CR' AND a.account_type IN ('COGS', 'Operating Expense')
GROUP BY c.cost_center_name ORDER BY expenses_crc DESC;

-- Q5
SELECT entity, COUNT(DISTINCT journal_id) AS journals, COUNT(*) AS lines
FROM silver_gl_entries GROUP BY entity;

-- Q6
SELECT entity, period, journal_id, account_id FROM silver_gl_entries
WHERE is_late_adjustment = 1;

-- Q7
SELECT reason, COUNT(*) AS rows_rejected FROM quarantine_gl_entries GROUP BY reason;

-- Q8
SELECT g.period, SUM(g.debit - g.credit) AS expenses_crc
FROM silver_gl_entries g
JOIN silver_dim_cost_center c ON c.cost_center_id = g.cost_center
JOIN silver_dim_account a ON a.account_id = g.account_id
WHERE g.entity = 'CR' AND c.cost_center_name = 'Sales'
  AND a.account_type IN ('COGS', 'Operating Expense')
  AND g.period IN (SELECT DISTINCT TOP 2 period FROM silver_gl_entries ORDER BY period DESC)
GROUP BY g.period;

-- Q9: compute expenses by account for the last 3 periods vs the previous 3 periods
-- (entity CO) and take the largest increase.

-- Q10: expected answer explains that EBITDA needs a definition and account mapping
-- that the data does not provide.
```

## Expected answers

| # | Expected answer | Agent answer | Score |
| --- | --- | --- | --- |
| 1 |  |  |  |
| 2 |  |  |  |
| 3 | Two figures, one per currency |  |  |
| 4 |  |  |  |
| 5 |  |  |  |
| 6 |  |  |  |
| 7 | 13 rows; breakdown by reason |  |  |
| 8 |  |  |  |
| 9 |  |  |  |
| 10 | Declines to compute; asks for the definition |  |  |

## Score history

| Date | Change made to the agent | Score (/10) |
| --- | --- | --- |
|  | Baseline: instructions only, no example queries |  |
|  | Added example queries |  |
