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
# META     },
# META     "warehouse": {}
# META   }
# META }

# CELL ********************

# nb_bronze_erp — Carga los CSV del ERP a Bronze sin transformar
# Copia cada bloque "# %%" en una celda distinta del notebook en Fabric.
# Lakehouse por defecto del notebook: andes_lakehouse

# %% CELDA 1 — Parámetros (márcala como "parameter cell" en el menú de la celda)
landing_date = "2026-10-12"
base_path = f"Files/erp/landing/{landing_date}"

# Ajusta los nombres si tu generador usa otros
source_files = {
    "bronze_erp_gl_entries": "journal_entries.csv",
    "bronze_erp_accounts": "chart_of_accounts.csv",
    "bronze_erp_cost_centers": "cost_centers.csv",
}

# %% CELDA 2 — Funciones
import uuid
from pyspark.sql import functions as F

batch_id = str(uuid.uuid4())


def read_raw_csv(file_name: str):
    """Lee el CSV tal cual: todo como texto, sin inferir tipos. Bronze no interpreta."""
    return (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .csv(f"{base_path}/{file_name}")
    )


def add_audit_columns(df, file_name: str):
    """Columnas de auditoría: cuándo, en qué corrida y desde qué archivo llegó cada fila."""
    return (
        df.withColumn("ingested_at", F.current_timestamp())
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("source_file", F.lit(f"{base_path}/{file_name}"))
    )


# %% CELDA 3 — Carga (append: Bronze guarda todo el historial de cargas)
for table_name, file_name in source_files.items():
    df = add_audit_columns(read_raw_csv(file_name), file_name)
    df.write.mode("append").format("delta").saveAsTable(table_name)
    print(f"{table_name}: {df.count()} filas cargadas (batch {batch_id})")

# %% CELDA 4 — Verificación
display(
    spark.sql("""
        SELECT batch_id, MIN(ingested_at) AS ingested_at, COUNT(*) AS filas
        FROM bronze_erp_gl_entries
        GROUP BY batch_id
        ORDER BY ingested_at DESC
    """)
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
