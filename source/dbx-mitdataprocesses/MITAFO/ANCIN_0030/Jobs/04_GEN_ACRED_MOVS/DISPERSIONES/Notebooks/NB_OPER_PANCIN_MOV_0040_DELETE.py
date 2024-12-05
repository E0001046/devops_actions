# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC %reload_ext autoreload
# MAGIC import sys
# MAGIC import configparser
# MAGIC import logging
# MAGIC import inspect
# MAGIC from pyspark.sql.functions import count, lit, current_timestamp
# MAGIC from pyspark.sql import SparkSession
# MAGIC from pyspark.sql.functions import col
# MAGIC from pyspark.sql.types import IntegerType, StringType
# MAGIC
# MAGIC # Configuración del logger
# MAGIC logging.getLogger().setLevel(logging.INFO)
# MAGIC logger = logging.getLogger("py4j")
# MAGIC logger.setLevel(logging.WARN)
# MAGIC logger = logging.getLogger(__name__)
# MAGIC
# MAGIC prod = True  # Para setear paths
# MAGIC
# MAGIC # Variables globales
# MAGIC
# MAGIC root_repo = "/Workspace/Shared/MITAFO"
# MAGIC config_files = {
# MAGIC     "general": f"{root_repo}/CGRLS_0010/Conf/CF_GRLS_PROCESS.py.properties",
# MAGIC     "connection": f"{root_repo}/CGRLS_0010/Conf/CF_GRLS_CONN.py.properties",
# MAGIC     "process": f"{root_repo}/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/DISPERSIONES/Conf/CF_PART_PROC.py.properties"
# MAGIC     if prod
# MAGIC     else "/Workspace/Repos/mronboye@emeal.nttdata.com/QueryConfigLab.ide/"
# MAGIC     "MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/"
# MAGIC     "DISPERSIONES/Conf/"
# MAGIC     "CF_PART_PROC.py.properties",
# MAGIC }
# MAGIC
# MAGIC notebook_name = (
# MAGIC     dbutils.notebook.entry_point.getDbutils()
# MAGIC     .notebook()
# MAGIC     .getContext()
# MAGIC     .notebookPath()
# MAGIC     .get()
# MAGIC )
# MAGIC message = "NB Error: " + notebook_name
# MAGIC source = "ETL"
# MAGIC
# MAGIC process_name = "root"
# MAGIC
# MAGIC # Carga de funciones externas
# MAGIC sys.path.append(f"{root_repo}/CGRLS_0010/Notebooks")
# MAGIC try:
# MAGIC     from NB_GRLS_DML_FUNCTIONS import *
# MAGIC     from NB_GRLS_SIMPLE_FUNCTIONS import *
# MAGIC except Exception as e:
# MAGIC     logger.error("Error al cargar funciones externas: %s", e)
# MAGIC
# MAGIC global_params = {}
# MAGIC global_confs = {}  # Diccionario para almacenar las keys globales
# MAGIC
# MAGIC
# MAGIC def input_values() -> dict:
# MAGIC     """Obtiene los valores de los widgets de entrada y los almacena en un diccionario global."""
# MAGIC
# MAGIC     widget_defaults = {
# MAGIC         "SR_FOLIO_REL": "",
# MAGIC         "SR_PROCESO": "",
# MAGIC         "SR_FECHA_LIQ": "",
# MAGIC         "SR_TIPO_MOV": "",
# MAGIC         "SR_REPROCESO": "",
# MAGIC         "SR_SUBPROCESO": "",
# MAGIC         "SR_USUARIO": "",
# MAGIC         "SR_INSTANCIA_PROCESO": "",
# MAGIC         "SR_ORIGEN_ARC": "",
# MAGIC         "SR_ID_SNAPSHOT": "",
# MAGIC         "SR_FECHA_ACC": "",
# MAGIC         "SR_FOLIO": "",
# MAGIC         "SR_SUBETAPA": "",
# MAGIC         "SR_FACTOR": "",
# MAGIC         "SR_ETAPA": "",
# MAGIC     }
# MAGIC
# MAGIC     # Crear los widgets en minúsculas
# MAGIC     for key, default_value in widget_defaults.items():
# MAGIC         dbutils.widgets.text(key.lower(), default_value)
# MAGIC
# MAGIC     # Actualizar el diccionario global en mayúsculas para el resto del notebook
# MAGIC     global_params.update(
# MAGIC         {key.upper(): dbutils.widgets.get(key.lower()).strip() for key in widget_defaults}
# MAGIC     )
# MAGIC
# MAGIC     if any(not value for value in global_params.values()):
# MAGIC         logger.error("Valores de entrada vacíos o nulos")
# MAGIC         global_params["status"] = "0"
# MAGIC     else:
# MAGIC         global_params["status"] = "1"
# MAGIC
# MAGIC     return global_params
# MAGIC
# MAGIC
# MAGIC def conf_process_values(arg_config_file: str, arg_process_name: str) -> tuple:
# MAGIC     """Obtiene los valores de configuración del proceso y los almacena en un diccionario global."""
# MAGIC     keys = [
# MAGIC         "sql_conf_file",
# MAGIC         "debug",
# MAGIC         "conn_schema_001",
# MAGIC         "conn_schema_002",
# MAGIC         "table_002",
# MAGIC         "table_011",
# MAGIC         "catalog_name",
# MAGIC         "schema_name",
# MAGIC     ]
# MAGIC
# MAGIC     try:
# MAGIC         config = configparser.ConfigParser()
# MAGIC         config.read(arg_config_file)
# MAGIC         result = {key: config.get(arg_process_name, key) for key in keys}
# MAGIC         result["status"] = "1"
# MAGIC         # Almacenar los valores en el diccionario global
# MAGIC         global_confs.update(result)
# MAGIC     except (ValueError, IOError) as error:
# MAGIC         logger.error("Error en la función %s: %s", inspect.stack()[0][3], error)
# MAGIC         result = {key: "0" for key in keys}
# MAGIC         result["status"] = "0"
# MAGIC         # Almacenar los valores en el diccionario global
# MAGIC         global_confs.update(result)
# MAGIC
# MAGIC     return tuple(result.values())
# MAGIC
# MAGIC # Configuración del manejador global de excepciones
# MAGIC def global_exception_handler(exc_type, exc_value, exc_traceback):
# MAGIC     if issubclass(exc_type, KeyboardInterrupt):
# MAGIC         # Permitir que KeyboardInterrupt se maneje normalmente
# MAGIC         sys.__excepthook__(exc_type, exc_value, exc_traceback)
# MAGIC         return
# MAGIC
# MAGIC     message = f"Uncaught exception: {exc_value}"
# MAGIC     source = "ETL"
# MAGIC     input_parameters = dbutils.widgets.getAll().items()
# MAGIC     # Registro del error y notificación
# MAGIC     logger.error("Please review log messages")
# MAGIC     notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC     raise Exception("An error raised")
# MAGIC
# MAGIC # Asigna el manejador de excepciones al hook global de sys
# MAGIC sys.excepthook = global_exception_handler
# MAGIC
# MAGIC
# MAGIC if __name__ == "__main__":
# MAGIC     # Inicialización de variables
# MAGIC     input_values()
# MAGIC     if global_params["status"] == "0":
# MAGIC         logger.error("Revisar mensajes en los logs")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Error en los valores de entrada, revisar logs")
# MAGIC
# MAGIC     input_parameters = dbutils.widgets.getAll().items()
# MAGIC
# MAGIC     webhook_url, channel, failed_task = conf_init_values(
# MAGIC         config_files["general"], process_name, "TEMP_PROCESS"
# MAGIC     )
# MAGIC
# MAGIC     if failed_task == "0":
# MAGIC         logger.error("Please review log MESSAGEs")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     process_name = "root"
# MAGIC     conf_values = conf_process_values(config_files["process"], process_name)
# MAGIC     if conf_values[-1] == "0":
# MAGIC         logger.error("Revisar mensajes en los logs")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Error en la configuración del proceso, revisar logs")
# MAGIC
# MAGIC     conn_name_ora = "jdbc_oracle"
# MAGIC     (
# MAGIC         conn_options,
# MAGIC         conn_additional_options,
# MAGIC         conn_user,
# MAGIC         conn_key,
# MAGIC         conn_url,
# MAGIC         scope,
# MAGIC         failed_task,
# MAGIC     ) = conf_conn_values(config_files["connection"], conn_name_ora)
# MAGIC     if failed_task == "0":
# MAGIC         logger.error("Revisar mensajes en los logs")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Error en la configuración de la conexión, revisar logs")
# MAGIC
# MAGIC     if prod:
# MAGIC         sql_conf_file = f"{root_repo}/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/DISPERSIONES/JSON/{conf_values[0]}"
# MAGIC     else:
# MAGIC         sql_conf_file = f"/Workspace/Repos/mronboye@emeal.nttdata.com/QueryConfigLab.ide/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/DISPERSIONES/JSON/{conf_values[0]}"
# MAGIC     # Seteamos el valor de debug
# MAGIC     debug = conf_values[1]
# MAGIC     debug = debug.lower() == 'true'

# COMMAND ----------

with open(sql_conf_file) as f:
    file_config_sql = json.load(f)

conf_values = [
    (fields["step_id"], "\n".join(fields["value"]))
    for line, value in file_config_sql.items()
    if line == "steps"
    for fields in value
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Leemos el tabla `ORACLE_DISPERSIONES`

# COMMAND ----------

catalog_name = f"{global_confs['catalog_name']}"
schema_name = f"{global_confs['schema_name']}"
table_name = f"oracle_dispersiones_{global_params['SR_FOLIO']}"

# Create the DataFrame DF_DISPERSIONES_03
DF_DISPERSIONES_03 = spark.sql(f"SELECT * FROM {catalog_name}.{schema_name}.{table_name}")

# Inserto DF_02_DISPERSIONES al cache
DF_DISPERSIONES_03.cache()

if debug:
    display(DF_DISPERSIONES_03)

# COMMAND ----------

from pyspark.sql.functions import lit

# Add the column LLAVE with value 1 to DF_DISPERSIONES_03
DF_DISPERSIONES_03 = DF_DISPERSIONES_03.withColumn("LLAVE", lit(1))

if debug:
    display(DF_DISPERSIONES_03)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hacemos un delete a la tabla `CIERREN_ETL.TTSISGRAL_ETL_MOVIMIENTOS`

# COMMAND ----------

target_table = f"{global_confs['conn_schema_001']}.{global_confs['table_002']}" # CIERREN_ETL.TTSISGRAL_ETL_MOVIMIENTOS
statement = f"""
DELETE FROM {target_table}
WHERE FTC_FOLIO = '{global_params['SR_FOLIO']}'
	AND FTN_MOV_GENERADO = 0
	AND FCN_ID_CONCEPTO_MOV IN (637,638,231)
"""

if debug:
    print(statement)

# COMMAND ----------

spark.conf.set("conn_url", str(conn_url))
spark.conf.set("conn_user", str(conn_user))
spark.conf.set("conn_key", str(conn_key))
spark.conf.set("statement", str(statement))
spark.conf.set("scope", str(scope))

# COMMAND ----------

# MAGIC %scala
# MAGIC import java.util.Properties
# MAGIC import java.sql.DriverManager
# MAGIC
# MAGIC val conn_user = spark.conf.get("conn_user")
# MAGIC val conn_key = spark.conf.get("conn_key")
# MAGIC val conn_url = spark.conf.get("conn_url")
# MAGIC val conn_scope = spark.conf.get("scope")
# MAGIC val driverClass = "oracle.jdbc.driver.OracleDriver"
# MAGIC
# MAGIC Class.forName(driverClass)
# MAGIC
# MAGIC val connectionProperties = new Properties()
# MAGIC var connection: java.sql.Connection = null // Declare connection outside the try block
# MAGIC
# MAGIC try {
# MAGIC     connectionProperties.setProperty("user", dbutils.secrets.get(scope=conn_scope, key=conn_user))
# MAGIC     connectionProperties.setProperty("password", dbutils.secrets.get(scope=conn_scope, key=conn_key))
# MAGIC     connectionProperties.setProperty("v$session.osuser", dbutils.secrets.get(scope=conn_scope, key=conn_user))
# MAGIC
# MAGIC     connection = DriverManager.getConnection(conn_url, connectionProperties) // Initialize connection here
# MAGIC     val stmt = connection.createStatement()
# MAGIC     val sql = spark.conf.get("statement")
# MAGIC
# MAGIC     stmt.execute(sql)
# MAGIC     spark.conf.set("failed_task", "1")
# MAGIC } catch {
# MAGIC     case _: Throwable => spark.conf.set("failed_task", "0")
# MAGIC } finally {
# MAGIC     if (connection != null) connection.close() // Check if connection is not null before closing
# MAGIC }

# COMMAND ----------

#Manejo de errores de la operación realizada en Scala

failed_task = spark.conf.get("failed_task")

if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hacemos una extraccion a la tabla `CIERREN.TTAFOGRAL_BALANCE_MOVS`

# COMMAND ----------

query_statement = "012"

table_name_001 = f"{global_confs['conn_schema_002']}.{global_confs['table_011']}" # CIERREN.TTAFOGRAL_BALANCE_MOVS

params = [
    table_name_001,
]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == "0":
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, global_params)
    raise Exception("Process ends")

# Ensure the statement is correctly formatted with the parameters
formatted_statement = statement.format(*params)

df, failed_task = query_table(
    conn_name_ora, spark, formatted_statement, conn_options, conn_user, conn_key
)

# Inserto DF_100_CRE_ETL_DISPERSION al cache
df.cache()

if failed_task == "0":
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")

if debug:
    display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hacemos un join por la key LLAVE entre `DF_DISPERSIONES_03` y `df`

# COMMAND ----------

# Perform the join on the key 'LLAVE' and select all columns from DF_DISPERSIONES_03 and MAXIMO1 from df
joined_df = DF_DISPERSIONES_03.alias("a").join(
    df.alias("b"),
    on="LLAVE",
    how="inner"
).select(
    "a.*",
    "b.MAXIMO1"
)

# unpersist DF_DISPERSIONES_03
DF_DISPERSIONES_03.unpersist()
del DF_DISPERSIONES_03

# Insertamos joined_df al cache
joined_df.cache()

if debug:
    # Display the resulting DataFrame
    display(joined_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora calculamos estas variables:
# MAGIC - ID = 1
# MAGIC - CONTADOR = MAXIMO1 + ID

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import row_number, col

# Define the window specification with partitioning and ordering
window_spec = Window.partitionBy("FTC_FOLIO").orderBy("FCD_FEH_CRE")

# Create the ID column as a consecutive number starting from 1
joined_df = joined_df.withColumn("ID", row_number().over(window_spec))

# Create the CONTADOR column as the sum of ID and MAXIMO1
joined_df = joined_df.withColumn("CONTADOR", col("ID") + col("MAXIMO1"))

# Display the result (optional)
if debug:
    display(joined_df)



# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora aplicamos estas transformaciones:
# MAGIC ```text
# MAGIC FTN_ID_BAL_MOV = CONTADOR
# MAGIC FTC_FOLIO = FTC_FOLIO
# MAGIC FTC_FOLIO_REL = FTC_FOLIO_REL
# MAGIC FTN_DISP_PESOS = 0
# MAGIC FTN_DISP_ACCIONES = If FTN_DISP_ACCIONES = 0 Then Else (FTN_DISP_ACCIONES * -1)
# MAGIC FTN_PDTE_PESOS = If FTN_PDTE_PESOS = 0 Then Else (FTN_PDTE_PESOS * -1)
# MAGIC FTN_PDTE_ACCIONES = If FTN_PDTE_ACCIONES = 0 Then Else (FTN_PDTE_ACCIONES * -1)
# MAGIC FTN_COMP_PESOS = If FTN_COMP_PESOS = 0 Then Else (FTN_COMP_PESOS * -1)
# MAGIC FTN_COMP_ACCIONES = If FTN_COMP_ACCIONES = 0 Then Else (FTN_COMP_ACCIONES * -1)
# MAGIC FTN_DIA_PESOS = 0
# MAGIC FTN_DIA_ACCIONES = 0
# MAGIC FTC_NUM_CTA_INVDUAL = FTC_NUM_CTA_INVDUAL
# MAGIC FTN_ORIGEN_APORTACION = SetNull()
# MAGIC FCN_ID_TIPO_SUBCTA = FCN_ID_TIPO_SUBCTA
# MAGIC FCN_ID_SIEFORE = FCN_ID_SIEFORE
# MAGIC FCN_ID_VALOR_ACCION = FCN_ID_VALOR_ACCION
# MAGIC FTD_FEH_LIQUIDACION = FTD_FEH_LIQUIDACION
# MAGIC FCN_ID_TIPO_MOV = FCN_ID_TIPO_MOV
# MAGIC FCN_ID_CONCEPTO_MOV = FCN_ID_CONCEPTO_MOV
# MAGIC FCC_TABLA_NCI_MOV = FCC_TABLA_NCI_MOV
# MAGIC FCD_FEH_CRE = Current Time Stamp()
# MAGIC FCC_USU_CRE = FCC_USU_CRE
# MAGIC FCD_FEH_ACT = Current Time Stamp()
# MAGIC FCC_USU_ACT = FCC_USU_ACT
# MAGIC FTN_DEDUCIBLE = FTN_DEDUCIBLE
# MAGIC FCN_ID_PLAZO = FCN_ID_PLAZO
# MAGIC ```

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import datetime
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    from_utc_timestamp,
    to_timestamp,
)

# Aplicar las reglas al DataFrame
joined_df = (
    joined_df.withColumn("FTN_ID_BAL_MOV", col("CONTADOR"))
    .withColumn("FTC_FOLIO", col("FTC_FOLIO"))
    .withColumn("FTC_FOLIO_REL", col("FTC_FOLIO_REL"))
    .withColumn("FTN_DISP_PESOS", F.lit(0))
    .withColumn(
        "FTN_DISP_ACCIONES",
        F.when(col("FTN_DISP_ACCIONES") == 0, col("FTN_DISP_ACCIONES")).otherwise(
            col("FTN_DISP_ACCIONES") * -1
        ),
    )
    .withColumn(
        "FTN_PDTE_PESOS",
        F.when(col("FTN_PDTE_PESOS") == 0, col("FTN_PDTE_PESOS")).otherwise(
            col("FTN_PDTE_PESOS") * -1
        ),
    )
    .withColumn(
        "FTN_PDTE_ACCIONES",
        F.when(col("FTN_PDTE_ACCIONES") == 0, col("FTN_PDTE_ACCIONES")).otherwise(
            col("FTN_PDTE_ACCIONES") * -1
        ),
    )
    .withColumn(
        "FTN_COMP_PESOS",
        F.when(col("FTN_COMP_PESOS") == 0, col("FTN_COMP_PESOS")).otherwise(
            col("FTN_COMP_PESOS") * -1
        ),
    )
    .withColumn(
        "FTN_COMP_ACCIONES",
        F.when(col("FTN_COMP_ACCIONES") == 0, col("FTN_COMP_ACCIONES")).otherwise(
            col("FTN_COMP_ACCIONES") * -1
        ),
    )
    .withColumn("FTN_DIA_PESOS", F.lit(0))
    .withColumn("FTN_DIA_ACCIONES", F.lit(0))
    .withColumn("FTC_NUM_CTA_INVDUAL", col("FTC_NUM_CTA_INVDUAL"))
    .withColumn("FTN_ORIGEN_APORTACION", F.lit(None).cast(T.StringType()))
    .withColumn("FCN_ID_TIPO_SUBCTA", col("FCN_ID_TIPO_SUBCTA"))
    .withColumn("FCN_ID_SIEFORE", col("FCN_ID_SIEFORE"))
    .withColumn("FCN_ID_VALOR_ACCION", col("FCN_ID_VALOR_ACCION"))
    .withColumn("FTD_FEH_LIQUIDACION", col("FTD_FEH_LIQUIDACION"))
    .withColumn("FCN_ID_TIPO_MOV", col("FCN_ID_TIPO_MOV"))
    .withColumn("FCN_ID_CONCEPTO_MOV", col("FCN_ID_CONCEPTO_MOV"))
    .withColumn("FCC_TABLA_NCI_MOV", col("FCC_TABLA_NCI_MOV"))
    .withColumn(
        "FCD_FEH_CRE", F.to_timestamp(from_utc_timestamp(current_timestamp(), "GMT-6"))
    )
    .withColumn("FCC_USU_CRE", col("FCC_USU_CRE"))
    .withColumn(
        "FCD_FEH_ACT", F.to_timestamp(from_utc_timestamp(current_timestamp(), "GMT-6"))
    )
    .withColumn("FCC_USU_ACT", col("FCC_USU_ACT"))
    .withColumn("FTN_DEDUCIBLE", col("FTN_DEDUCIBLE"))
    .withColumn("FCN_ID_PLAZO", col("FCN_ID_PLAZO"))
)

# Mostrar el resultado (opcional)
if debug:
    display(joined_df)

# COMMAND ----------

from pyspark.sql import functions as F

columns_to_select = [
    "FTN_ID_BAL_MOV",
    "FTN_DISP_PESOS",
    "FTN_DISP_ACCIONES",
    "FTN_PDTE_PESOS",
    "FTN_PDTE_ACCIONES",
    "FTN_COMP_PESOS",
    "FTN_COMP_ACCIONES",
    "FTN_DIA_PESOS",
    "FTN_DIA_ACCIONES",
    "FTN_ORIGEN_APORTACION",
    "FCD_FEH_CRE",
    "FCD_FEH_ACT",
    "FTC_FOLIO",
    "FTC_NUM_CTA_INVDUAL",
    "FCN_ID_TIPO_SUBCTA",
    "FCN_ID_TIPO_MOV",
    "FCN_ID_CONCEPTO_MOV",
    "FCC_TABLA_NCI_MOV",
    "FTC_FOLIO_REL",
    "FTD_FEH_LIQUIDACION",
    "FCC_USU_CRE",
    "FCC_USU_ACT",
    "FCN_ID_SIEFORE",
    "FCN_ID_VALOR_ACCION",
    "FTN_DEDUCIBLE",
    "FCN_ID_PLAZO"
]

joined_df = joined_df.withColumn("FTC_FOLIO", F.split(F.col("FTC_FOLIO"), "\.")[0])

# Seleccionar las columnas del DataFrame
selected_df = joined_df.select(*columns_to_select)

# Liberar la caché del DataFrame si se usó cache
joined_df.unpersist()
del joined_df

# Mostrar el resultado (opcional)
if debug:
    selected_df.printSchema()
    display(selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insertamos en `CIERREN.TTAFOGRAL_BALANCE_MOVS`

# COMMAND ----------

target_table = f"{global_confs['conn_schema_002']}.{global_confs['table_011']}" # CIERREN.TTAFOGRAL_BALANCE_MOVS
mode = "APPEND"
failed_task = write_into_table(
    conn_name_ora,
    selected_df,
    mode,
    target_table,
    conn_options,
    conn_additional_options,
    conn_user,
    conn_key,
)

if failed_task == "0":
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")

# COMMAND ----------

from pyspark.sql import DataFrame

# Clear cache
spark.catalog.clearCache()

# Unpersist and delete all DataFrames
for df_name in list(globals()):
    if isinstance(globals()[df_name], DataFrame):
        globals()[df_name].unpersist()
        del globals()[df_name]

# COMMAND ----------

# Liberar la caché del DataFrame si se usó cache
selected_df.unpersist()

# Eliminar DataFrames para liberar memoria
del selected_df

# Recolector de basura para liberar recursos inmediatamente
import gc
gc.collect()
