# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "14d9ad92-a745-4799-9f33-72d8c035bd14",
# META       "default_lakehouse_name": "andes_lakehouse",
# META       "default_lakehouse_workspace_id": "ecdac935-1c4c-4447-ada0-35a2abb02cde",
# META       "known_lakehouses": [
# META         {
# META           "id": "14d9ad92-a745-4799-9f33-72d8c035bd14"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# nb_silver_erp — De Bronze a Silver con validaciones, cuarentena, MERGE y watermark
# Copia cada bloque "# %%" en una celda distinta del notebook en Fabric.
# Lakehouse por defecto del notebook: andes_lakehouse
# Ajusta los nombres de columnas de las dimensiones si tu generador usa otros.

# %% CELDA 1 — Parámetros
max_quarantine_ratio = 0.05   # si más del 5 % de las filas falla, no se promueve nada a Silver
late_adjustment_days = 10     # días después del cierre de un período para considerarlo ajuste tardío

# %% CELDA 2 — Imports y funciones auxiliares
# Requiere andes_lib instalado en el Environment del notebook (wheel de este repo).
import uuid
from delta.tables import DeltaTable
from pyspark.sql import functions as F

from andes_lib.audit import add_row_hash
from andes_lib.dedupe import keep_latest
from andes_lib.finance.checks import flag_late_adjustments, validate_gl

run_id = str(uuid.uuid4())


def latest_batch(table_name: str):
    """Filas de la última carga de una tabla Bronze (para dimensiones que llegan completas)."""
    df = spark.table(table_name)
    last = df.agg(F.max("ingested_at")).collect()[0][0]
    return df.filter(F.col("ingested_at") == last).drop("ingested_at", "batch_id", "source_file")


# %% CELDA 3 — Dimensiones (llegan completas en cada carga: overwrite)
latest_batch("bronze_erp_accounts").write.mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("silver_dim_account")

latest_batch("bronze_erp_cost_centers").write.mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("silver_dim_cost_center")

# %% CELDA 4 — Watermark: solo procesar lo que llegó a Bronze desde la última corrida
spark.sql("""
    CREATE TABLE IF NOT EXISTS ctl_watermark (
        source_table STRING, last_watermark TIMESTAMP, last_run_at TIMESTAMP
    )
""")

wm_rows = spark.sql(
    "SELECT last_watermark FROM ctl_watermark WHERE source_table = 'bronze_erp_gl_entries'"
).collect()
last_watermark = wm_rows[0][0] if wm_rows else None

src = spark.table("bronze_erp_gl_entries")
if last_watermark is not None:
    src = src.filter(F.col("ingested_at") > F.lit(last_watermark))

print(f"Watermark anterior: {last_watermark} | filas nuevas en Bronze: {src.count()}")

# %% CELDA 5 — Tipado, hash y deduplicación
hash_cols = ["entity", "period", "account_id", "cost_center", "debit", "credit", "currency"]

typed = add_row_hash(
    src.select(
        "entry_id",
        "journal_id",
        "entity",
        "period",
        F.to_date("posting_date").alias("posting_date"),
        "account_id",
        "cost_center",
        F.col("debit").cast("decimal(18,2)").alias("debit"),
        F.col("credit").cast("decimal(18,2)").alias("credit"),
        "currency",
        F.to_timestamp("source_updated_at").alias("source_updated_at"),
        "ingested_at",
        "batch_id",
    ),
    hash_cols,
)

# Si un asiento llegó más de una vez, quedarse con la versión más reciente
gl = keep_latest(typed, "entry_id", "source_updated_at").cache()

# %% CELDA 6 — Validaciones fila por fila (el motivo queda escrito en la columna reason)
valid_accounts = spark.table("silver_dim_account").select("account_id")

# Ajuste tardío: el período ya cerró y el registro se modificó días después del cierre.
# No es un error contable, así que no va a cuarentena: se marca para que sea visible.
good, bad = validate_gl(flag_late_adjustments(gl, late_adjustment_days), valid_accounts)

total_rows, bad_rows = gl.count(), bad.count()
print(f"Filas: {total_rows} | válidas: {total_rows - bad_rows} | cuarentena: {bad_rows}")

# %% CELDA 7 — Cuarentena y log de calidad (se escriben siempre, pase lo que pase después)
(
    bad.withColumn("run_id", F.lit(run_id))
    .withColumn("quarantined_at", F.current_timestamp())
    .write.mode("append")
    .option("mergeSchema", "true")
    .saveAsTable("quarantine_gl_entries")
)

metrics = {
    "filas_leidas": total_rows,
    "filas_validas": total_rows - bad_rows,
    "filas_cuarentena": bad_rows,
    "ajustes_tardios": good.filter("is_late_adjustment").count(),
}
(
    spark.createDataFrame(
        [(run_id, "silver_gl_entries", k, int(v)) for k, v in metrics.items()],
        "run_id string, table_name string, metric string, value long",
    )
    .withColumn("logged_at", F.current_timestamp())
    .write.mode("append")
    .saveAsTable("ops_quality_log")
)

# %% CELDA 8 — Regla bloqueante: si falla demasiado, se detiene y no se toca Silver
if total_rows > 0 and bad_rows / total_rows > max_quarantine_ratio:
    raise ValueError(
        f"{bad_rows}/{total_rows} filas en cuarentena supera el umbral de "
        f"{max_quarantine_ratio:.0%}. Revisar quarantine_gl_entries (run_id {run_id})."
    )

# %% CELDA 9 — MERGE a Silver: inserta nuevos, actualiza solo si cambió de verdad
if not spark.catalog.tableExists("silver_gl_entries"):
    good.limit(0).write.format("delta").saveAsTable("silver_gl_entries")

(
    DeltaTable.forName(spark, "silver_gl_entries")
    .alias("t")
    .merge(good.alias("s"), "t.entry_id = s.entry_id")
    .whenMatchedUpdateAll(
        condition="s.source_updated_at > t.source_updated_at AND s.row_hash <> t.row_hash"
    )
    .whenNotMatchedInsertAll()
    .execute()
)

# %% CELDA 10 — Actualizar el watermark solo al final, si todo salió bien
new_watermark = src.agg(F.max("ingested_at")).collect()[0][0]
if new_watermark is not None:
    spark.sql("DELETE FROM ctl_watermark WHERE source_table = 'bronze_erp_gl_entries'")
    (
        spark.createDataFrame(
            [("bronze_erp_gl_entries", new_watermark)],
            "source_table string, last_watermark timestamp",
        )
        .withColumn("last_run_at", F.current_timestamp())
        .write.mode("append")
        .saveAsTable("ctl_watermark")
    )
print(f"Watermark nuevo: {new_watermark}")

# %% CELDA 11 — Verificación
display(spark.sql("""
    SELECT reason, COUNT(*) AS filas, COUNT(DISTINCT journal_id) AS asientos
    FROM quarantine_gl_entries
    WHERE run_id = '""" + run_id + """'
    GROUP BY reason
"""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM andes_lakehouse.dbo.silver_dim_cost_center LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
