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
# MAGIC         "table_003",
# MAGIC         "table_004",
# MAGIC         "table_005",
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
# MAGIC     input_parameters = dbutils.widgets.getAll().items()
# MAGIC
# MAGIC     webhook_url, channel, failed_task = conf_init_values(
# MAGIC         config_files["general"], process_name, "TEMP_PROCESS"
# MAGIC     )
# MAGIC
# MAGIC     input_values()
# MAGIC     if global_params["status"] == "0":
# MAGIC         logger.error("Revisar mensajes en los logs")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Error en los valores de entrada, revisar logs")
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
# MAGIC
# MAGIC     if global_params['SR_FOLIO_REL'].lower() == 'null':
# MAGIC         global_params['SR_FOLIO_REL'] = None

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
# MAGIC ### Creamos el `DF_02_DISPERSIONES` a partir de la vista temporal `TEMP_DISPERSION_MOV_02`

# COMMAND ----------

table_name = f"TEMP_DISPERSION_MOV_{global_params['SR_FOLIO']}"

# Create the DataFrame DF_02_DISPERSIONES from the temporary view TEMP_DISPERSION_MOV_02
DF_02_DISPERSIONES = spark.sql(f"SELECT * FROM {global_confs['catalog_name']}.{global_confs['schema_name']}.{table_name}")

# Inserto DF_02_DISPERSIONES al cache
DF_02_DISPERSIONES.cache()

if debug:
    display(DF_02_DISPERSIONES)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Contamos los registros semadoro rojo y semaforo verde
# MAGIC ```text
# MAGIC Rojo = FTN_ID_SEMAFORO_MOV = 191
# MAGIC Verde = FTN_ID_SEMAFORO_MOV = 190
# MAGIC ```

# COMMAND ----------

count_rojos = DF_02_DISPERSIONES.filter(DF_02_DISPERSIONES.FTN_ID_SEMAFORO_MOV==191).count()
count_verdes = DF_02_DISPERSIONES.filter(DF_02_DISPERSIONES.FTN_ID_SEMAFORO_MOV==190).count()

# unpersist DF_02_DISPERSIONES
DF_02_DISPERSIONES.unpersist()
del DF_02_DISPERSIONES

if debug:
    print(f"count_rojos: {count_rojos} - count_verdes: {count_verdes}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hacemos una extraccion de datos a la tabla `CIERREN.TTCRXGRAL_FOLIO`

# COMMAND ----------

query_statement = "008"
table_name_001 = f"{global_confs['conn_schema_002']}.{global_confs['table_003']}"  # CIERREN.TTCRXGRAL_FOLIO
table_name_002 = f"{global_confs['conn_schema_002']}.{global_confs['table_004']}"  # CIERREN.TRAFOGRAL_ARCHIVO_FOLIO

params = [
    count_rojos,
    count_verdes,
    table_name_001,
    table_name_002,
    global_params["SR_FOLIO"],
]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == "0":
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, global_params)
    raise Exception("Process ends")

# Ensure the statement is correctly formatted with the parameters
formatted_statement = statement.format(*params)

DF_0200_TRAFOGRAL_ARCHIVO_FOLIO, failed_task = query_table(
    conn_name_ora, spark, formatted_statement, conn_options, conn_user, conn_key
)

# Inserto DF_100_CRE_ETL_DISPERSION al cache
DF_0200_TRAFOGRAL_ARCHIVO_FOLIO.cache()

if failed_task == "0":
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")

if debug:
    display(DF_0200_TRAFOGRAL_ARCHIVO_FOLIO)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aplicamos estas reglas a `DF_0200_TRAFOGRAL_ARCHIVO_FOLIO`:
# MAGIC ```text
# MAGIC FTC_FOLIO = p_SR_FOLIO
# MAGIC FTC_ID_SUBETAPA = 273
# MAGIC FLN_TOTAL_REGISTROS = COUNT_VERDE + COUNT_ROJO
# MAGIC FLN_REG_CUMPLIERON = COUNT_VERDE
# MAGIC FLN_REG_NO_CUMPLIERON = COUNT_ROJO
# MAGIC FLC_VALIDACION = setnull()
# MAGIC FLN_TOTAL_ERRORES = setnull()
# MAGIC FLC_DETALLE = setnull()
# MAGIC FLD_FEC_REG = Current Timestamp()
# MAGIC FLC_USU_REG = p_CX_CRE_USUARIO
# MAGIC FTC_FOLIO_REL = p_SR_FOLIO_REL
# MAGIC FTN_ID_ARCHIVO = FTN_ID_ARCHIVO
# MAGIC ```

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    from_utc_timestamp,
    to_timestamp,
)

# Parámetros
p_SR_FOLIO = global_params["SR_FOLIO"]
p_CX_CRE_USUARIO = "EJE_CIERRENDATA"
p_SR_FOLIO_REL = global_params["SR_FOLIO_REL"]

# Aplicar las reglas al nuevo DataFrame DF_0200_TRAFOGRAL_ARCHIVO_FOLIO
DF_0200_TRAFOGRAL_ARCHIVO_FOLIO = (
    DF_0200_TRAFOGRAL_ARCHIVO_FOLIO.withColumn("FTC_FOLIO", F.lit(p_SR_FOLIO))
    .withColumn("FTC_ID_SUBETAPA", F.lit(273))
    .withColumn("FLN_TOTAL_REGISTROS", F.col("COUNT_VERDE") + F.col("COUNT_ROJO"))
    .withColumn("FLN_REG_CUMPLIERON", F.col("COUNT_VERDE"))
    .withColumn("FLN_REG_NO_CUMPLIERON", F.col("COUNT_ROJO"))
    .withColumn("FLC_VALIDACION", F.lit(None).cast("string"))
    .withColumn("FLN_TOTAL_ERRORES", F.lit(None).cast("integer"))
    .withColumn("FLC_DETALLE", F.lit(None).cast("string"))
    .withColumn(
        "FLD_FEC_REG", F.to_timestamp(from_utc_timestamp(current_timestamp(), "GMT-6"))
    )
    .withColumn("FLC_USU_REG", F.lit(p_CX_CRE_USUARIO))
    .withColumn("FTC_FOLIO_REL", F.lit(p_SR_FOLIO_REL))
    .withColumn("FTN_ID_ARCHIVO", F.col("FTN_ID_ARCHIVO"))
)

if debug:
    # Mostrar el resultado
    display(DF_0200_TRAFOGRAL_ARCHIVO_FOLIO)
    DF_0200_TRAFOGRAL_ARCHIVO_FOLIO.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hacemos un Update / Insert el resultado en la tabla `CIERREN.TLAFOGRAL_VAL_CIFRAS_CONTROL`

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Como approach nos iremos por el camino de borrar el registro y luego insertarlo. A continuacion, el borrado.

# COMMAND ----------

table_name_003 = f"{global_confs['conn_schema_002']}.{global_confs['table_005']}"  # CIERREN.TLAFOGRAL_VAL_CIFRAS_CONTROL

# Obtener el valor de la columna FTN_ID_ARCHIVO de DF_0200_TRAFOGRAL_ARCHIVO_FOLIO
ftn_id_archivo_row = DF_0200_TRAFOGRAL_ARCHIVO_FOLIO.select("FTN_ID_ARCHIVO").first()
if ftn_id_archivo_row is not None:
    ftn_id_archivo_value = ftn_id_archivo_row["FTN_ID_ARCHIVO"]
else:
    ftn_id_archivo_value = None

# Si FTC_FOLIO_REL = null crear ftc_folio_rel = None
if global_params["SR_FOLIO_REL"] is None:
    folio_rel = "null"
else:
    folio_rel = global_params["SR_FOLIO_REL"]

if ftn_id_archivo_value is not None:
    statement = f"""
        DELETE FROM {table_name_003}
        WHERE FTC_FOLIO = '{global_params["SR_FOLIO"]}'
            AND FTN_ID_ARCHIVO = {ftn_id_archivo_value}
            AND FTC_ID_SUBETAPA = 273
            AND FTC_FOLIO_REL = {folio_rel}
        """
else:
    statement = f"""
        DELETE FROM {table_name_003}
        WHERE FTC_FOLIO = '{global_params["SR_FOLIO"]}'
            AND FTN_ID_ARCHIVO IS NULL
            AND FTC_ID_SUBETAPA = 273
            AND FTC_FOLIO_REL = {folio_rel}
        """
if debug:
    print(statement)

# COMMAND ----------

sr_folio = '202411131746299198'
statement = f"""
DELETE FROM CIERREN.TTAFOGRAL_BALANCE_MOVS
WHERE FTC_FOLIO = '{sr_folio}'
"""

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
# MAGIC ### Y ahora hacemos el insert

# COMMAND ----------

# Lista de columnas que deseas seleccionar
columns_to_select = [
    "FTC_FOLIO",
    "FTC_ID_SUBETAPA",
    "FLN_TOTAL_REGISTROS",
    "FLN_REG_CUMPLIERON",
    "FLN_REG_NO_CUMPLIERON",
    "FLC_VALIDACION",
    "FLN_TOTAL_ERRORES",
    "FLC_DETALLE",
    "FLD_FEC_REG",
    "FLC_USU_REG",
    "FTC_FOLIO_REL",
    "FTN_ID_ARCHIVO"
]

DF_0200_TRAFOGRAL_ARCHIVO_FOLIO = DF_0200_TRAFOGRAL_ARCHIVO_FOLIO.select(*columns_to_select)

if debug:
    display(DF_0200_TRAFOGRAL_ARCHIVO_FOLIO)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ajustamos los datos para OCI

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType, IntegerType, TimestampType

# Ajuste de tipos y redondeo de decimales
DF_0200_TRAFOGRAL_ARCHIVO_FOLIO = DF_0200_TRAFOGRAL_ARCHIVO_FOLIO \
    .withColumn("FTC_FOLIO", F.col("FTC_FOLIO").cast(StringType())) \
    .withColumn("FTC_ID_SUBETAPA", F.col("FTC_ID_SUBETAPA").cast(DecimalType(22, 0))) \
    .withColumn("FLN_TOTAL_REGISTROS", F.col("FLN_TOTAL_REGISTROS").cast(DecimalType(22, 0))) \
    .withColumn("FLN_REG_CUMPLIERON", F.col("FLN_REG_CUMPLIERON").cast(DecimalType(22, 0))) \
    .withColumn("FLN_REG_NO_CUMPLIERON", F.col("FLN_REG_NO_CUMPLIERON").cast(DecimalType(22, 0))) \
    .withColumn("FLC_VALIDACION", F.col("FLC_VALIDACION").cast(StringType())) \
    .withColumn("FLN_TOTAL_ERRORES", F.col("FLN_TOTAL_ERRORES").cast(IntegerType())) \
    .withColumn("FLC_DETALLE", F.col("FLC_DETALLE").cast(StringType())) \
    .withColumn("FLD_FEC_REG", F.col("FLD_FEC_REG").cast(TimestampType())) \
    .withColumn("FLC_USU_REG", F.col("FLC_USU_REG").cast(StringType())) \
    .withColumn("FTC_FOLIO_REL", F.when(F.col("FTC_FOLIO_REL") == '', None).otherwise(F.col("FTC_FOLIO_REL")).cast(StringType())) \
    .withColumn("FTN_ID_ARCHIVO", F.col("FTN_ID_ARCHIVO").cast(DecimalType(22, 0)))

# Truncar cadenas a las longitudes necesarias para Oracle
DF_0200_TRAFOGRAL_ARCHIVO_FOLIO = DF_0200_TRAFOGRAL_ARCHIVO_FOLIO \
    .withColumn("FTC_FOLIO", F.expr("substring(FTC_FOLIO, 1, 30)")) \
    .withColumn("FLC_VALIDACION", F.expr("substring(FLC_VALIDACION, 1, 200)")) \
    .withColumn("FLC_DETALLE", F.expr("substring(FLC_DETALLE, 1, 300)")) \
    .withColumn("FLC_USU_REG", F.expr("substring(FLC_USU_REG, 1, 30)")) \
    .withColumn("FTC_FOLIO_REL", F.expr("substring(FTC_FOLIO_REL, 1, 30)"))

if debug:
    display(DF_0200_TRAFOGRAL_ARCHIVO_FOLIO)


# COMMAND ----------

mode = "APPEND"
failed_task = write_into_table(
    conn_name_ora,
    DF_0200_TRAFOGRAL_ARCHIVO_FOLIO,
    mode,
    table_name_003,
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
DF_0200_TRAFOGRAL_ARCHIVO_FOLIO.unpersist()

# Eliminar DataFrames para liberar memoria
del DF_0200_TRAFOGRAL_ARCHIVO_FOLIO

# Recolector de basura para liberar recursos inmediatamente
import gc
gc.collect()
