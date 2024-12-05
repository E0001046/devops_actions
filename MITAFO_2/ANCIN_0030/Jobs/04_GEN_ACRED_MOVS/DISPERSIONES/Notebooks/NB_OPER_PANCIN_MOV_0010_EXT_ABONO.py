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
# MAGIC     # Definir los widgets en minúsculas
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
# MAGIC     }
# MAGIC
# MAGIC     # Crear los widgets en minúsculas
# MAGIC     for key, default_value in widget_defaults.items():
# MAGIC         dbutils.widgets.text(key.lower(), default_value)
# MAGIC
# MAGIC     # Actualizar el diccionario global en mayúsculas para el resto del notebook
# MAGIC     global_params.update(
# MAGIC         {
# MAGIC             key.upper(): dbutils.widgets.get(key.lower()).strip()
# MAGIC             for key in widget_defaults
# MAGIC         }
# MAGIC     )
# MAGIC
# MAGIC     # Verificar si los valores son válidos
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
# MAGIC         "sql_conf_file",  # siempre de primero
# MAGIC         "debug",  # siempre de segundo
# MAGIC         "conn_schema_001",
# MAGIC         "conn_schema_002",
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
# MAGIC     # Inicialización de variables
# MAGIC     input_values()
# MAGIC     if global_params["status"] == "0":
# MAGIC         logger.error("Revisar mensajes en los logs")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Error en los valores de entrada, revisar logs")
# MAGIC
# MAGIC     if failed_task == "0":
# MAGIC         logger.error("Please review log messages")
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
# MAGIC     debug = debug.lower() == "true"

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
# MAGIC ### Extraccion a `CIERREN_ETL.TTSISGRAL_ETL_DISPERSION`

# COMMAND ----------

query_statement = "001"

params = [
    global_params["SR_FOLIO"],
    global_params["SR_FOLIO_REL"],
]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == "0":
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, global_params)
    raise Exception("Process ends")

# Ensure the statement is correctly formatted with the parameters
formatted_statement = statement.format(*params)

DF_100_CRE_ETL_DISPERSION, failed_task = query_table(
    conn_name_ora, spark, formatted_statement, conn_options, conn_user, conn_key
)

# Inserto DF_100_CRE_ETL_DISPERSION al cache
DF_100_CRE_ETL_DISPERSION.cache()

if failed_task == "0":
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")

if debug:
    display(DF_100_CRE_ETL_DISPERSION)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extraccion a `CIERREN.TTAFOGRAL_MATRIZ_CONVIVENCIA`

# COMMAND ----------

query_statement = "002"

params = [
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

DF_200_MATRIZ, failed_task = query_table(
    conn_name_ora, spark, formatted_statement, conn_options, conn_user, conn_key
)

# Inserto DF_200_MATRIZ al cache
DF_200_MATRIZ.cache()

if failed_task == "0":
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")

if debug:
    display(DF_200_MATRIZ)

# COMMAND ----------

# MAGIC %md
# MAGIC Se hace un inner join de las 2 consultas anteriores a traves de la key `FTN_NUM_CTA_INVDUAL` con el objetivo de mapear `ID_MARCA`

# COMMAND ----------

# Perform an inner join between DF_100_CRE_ETL_DISPERSION and DF_200_MATRIZ on the key FTN_NUM_CTA_INVDUAL
# The final DataFrame should include the key ID_MARCA

# Assuming the key columns are named 'FTN_NUM_CTA_INVDUAL' in both DataFrames
main_df = DF_100_CRE_ETL_DISPERSION.join(
    DF_200_MATRIZ,
    DF_100_CRE_ETL_DISPERSION["FTN_NUM_CTA_INVDUAL"] == DF_200_MATRIZ["FTN_NUM_CTA_INVDUAL"],
    "inner"
).select(DF_100_CRE_ETL_DISPERSION["*"], DF_200_MATRIZ["FTN_ID_MARCA"])

# unpersist the cache
DF_200_MATRIZ.unpersist()
DF_100_CRE_ETL_DISPERSION.unpersist()

# eliminar los df
del DF_200_MATRIZ, DF_100_CRE_ETL_DISPERSION

# Inserto main_df al cache
main_df.cache()

if debug:
    display(main_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extraccion a `CIERREN.TCCRXGRAL_TIPO_SUBCTA`

# COMMAND ----------

query_statement = "003"

params = [
]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == "0":
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, global_params)
    raise Exception("Process ends")

# Ensure the statement is correctly formatted with the parameters
formatted_statement = statement.format(*params)

DF_300_TipoSubcta, failed_task = query_table(
    conn_name_ora, spark, formatted_statement, conn_options, conn_user, conn_key
)

# Inserto DF_300_TipoSubcta al cache
DF_300_TipoSubcta.cache()

if failed_task == "0":
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")

if debug:
    display(DF_300_TipoSubcta)

# COMMAND ----------

# MAGIC %md
# MAGIC Hacemos un inner join entre `DF_300_TipoSubcta` y los datos del flujo (`main_df`) a través de la key `FCN_ID_TIPO_SUBCTA`, esto con el propósito de mapear los campos `ID_PLAZO` y `ID_REGIMEN`

# COMMAND ----------

# Perform an inner join between DF_300_TipoSubcta and main_df on the key FCN_ID_TIPO_SUBCTA
# The final DataFrame should include the fields ID_PLAZO and ID_REGIMEN

# Assuming the key columns are named 'FCN_ID_TIPO_SUBCTA' in both DataFrames
main_df = main_df.join(
    DF_300_TipoSubcta,
    main_df["FCN_ID_TIPO_SUBCTA"] == DF_300_TipoSubcta["FCN_ID_TIPO_SUBCTA"],
    "inner"
).select(main_df["*"], DF_300_TipoSubcta["FCN_ID_PLAZO"], DF_300_TipoSubcta["FCN_ID_REGIMEN"])

# unpersist the cache
DF_300_TipoSubcta.unpersist()

# eliminar los df
del DF_300_TipoSubcta

if debug:
    display(main_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Luego le hacemos un cambio al campo `FCN_ID_REGIMEN` según esta regla: 
# MAGIC ```sql
# MAGIC IF FCN_ID_SIEFORE = 82 then 140 ELSE
# MAGIC 	IF FCN_ID_SIEFORE = 83 then 140 ELSE FCN_ID_REGIMEN
# MAGIC ```

# COMMAND ----------

from pyspark.sql.functions import when, col

# Apply the rule to the main_df DataFrame
main_df = main_df.withColumn(
    "FCN_ID_REGIMEN",
    when(col("FCN_ID_SIEFORE") == 82, 140).otherwise(
        when(col("FCN_ID_SIEFORE") == 83, 140).otherwise(col("FCN_ID_REGIMEN"))
    ),
)

if debug:
    display(main_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extraccion a `CIERREN.TCAFOGRAL_VALOR_ACCION`

# COMMAND ----------

query_statement = "004"

params = [
    global_params["SR_FECHA_ACC"]
]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == "0":
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, global_params)
    raise Exception("Process ends")

# Ensure the statement is correctly formatted with the parameters
formatted_statement = statement.format(*params)

DF_400_valorAccion, failed_task = query_table(
    conn_name_ora, spark, formatted_statement, conn_options, conn_user, conn_key
)

# Inserto DF_400_valorAccion al cache
DF_400_valorAccion.cache()

if failed_task == "0":
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")

if debug:
    display(DF_400_valorAccion)

# COMMAND ----------

# MAGIC %md
# MAGIC Luego hacemos un inner join entre los datos del flujo (`main_df`) y la tabla `DB_400_valorAccion` con la finalidad de mapear los campos que se ven la imagen (`FCN_ID_VALOR_ACCION` y `FCN_VALOR_ACCION`). El inner join lo hacemos a través las keys `FCN_ID_SIEFORE` y `FCN_ID_REGIMEN`

# COMMAND ----------

# Perform an inner join between main_df and DF_400_valorAccion on FCN_ID_SIEFORE and FCN_ID_REGIMEN
main_df = main_df.join(
    DF_400_valorAccion,
    (main_df["FCN_ID_SIEFORE"] == DF_400_valorAccion["FCN_ID_SIEFORE"]) &
    (main_df["FCN_ID_REGIMEN"] == DF_400_valorAccion["FCN_ID_REGIMEN"]),
    "inner"
).select(
    main_df["*"],
    DF_400_valorAccion["FCN_ID_VALOR_ACCION"],
    DF_400_valorAccion["FCN_VALOR_ACCION"]
)

# unpersist the cache
DF_400_valorAccion.unpersist()

# eliminar los df
del DF_400_valorAccion

if debug:
    display(main_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extraccion a `CIERREN.TFAFOGRAL_CONFIG_CONCEP_MOV`

# COMMAND ----------

query_statement = "005"

params = [
]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == "0":
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, global_params)
    raise Exception("Process ends")

# Ensure the statement is correctly formatted with the parameters
formatted_statement = statement.format(*params)

DF_500_ConfigConcept, failed_task = query_table(
    conn_name_ora, spark, formatted_statement, conn_options, conn_user, conn_key
)

# Inserto DF_500_ConfigConcept al cache
DF_500_ConfigConcept.cache()

if failed_task == "0":
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")

if debug:
    display(DF_500_ConfigConcept)

# COMMAND ----------

# MAGIC %md
# MAGIC Luego hacemos un inner join del `df_main` con `DF_500_ConfigConcept` a través de la key `FNN_ID_CONCEPTO_MOV`, con la finalidad de traernos el campo `FTN_DEDUCIBLE`

# COMMAND ----------

# Perform an inner join between main_df and DF_500_ConfigConcept on FNN_ID_CONCEPTO_MOV
main_df = main_df.join(
    DF_500_ConfigConcept,
    main_df["FFN_ID_CONCEPTO_MOV"] == DF_500_ConfigConcept["FFN_ID_CONCEPTO_MOV"],
    "inner",
).select(main_df["*"], DF_500_ConfigConcept["FTN_DEDUCIBLE"])

# unpersist the cache
DF_500_ConfigConcept.unpersist()

# eliminar los df
del DF_500_ConfigConcept

if debug:
    display(main_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extraccion a `CIERREN_ETL.TTSISGRAL_ETL_DISPERSION`

# COMMAND ----------

query_statement = "006"

params = [
    global_params["SR_FOLIO"]
]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == "0":
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, global_params)
    raise Exception("Process ends")

# Ensure the statement is correctly formatted with the parameters
formatted_statement = statement.format(*params)

DF_100_CRE_ETL_DISPERSION_VIV, failed_task = query_table(
    conn_name_ora, spark, formatted_statement, conn_options, conn_user, conn_key
)

# Inserto DF_100_CRE_ETL_DISPERSION_VIV al cache
DF_100_CRE_ETL_DISPERSION_VIV.cache()

if failed_task == "0":
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")

if debug:
    display(DF_100_CRE_ETL_DISPERSION_VIV)

# COMMAND ----------

# MAGIC %md
# MAGIC Hacemos un left join de `main_df` con `DF_100_CRE_ETL_DISPERSION_VIV` a través de los campos: `FCN_ID_TIPO_SUBCTA`, `FCN_ID_SIEFORE`, `FTN_NUM_CTA_INVDUAL`, `FTC_TABLA_NCI_MOV`, `FTN_NO_LINEA` y `FFN_ID_CONCEPTO_MOV`. Del resultado final solo nos traemos la key `FTF_MONTO_ACCIONES`.

# COMMAND ----------

# Perform a left join between main_df and DF_100_CRE_ETL_DISPERSION_VIV on specified fields
main_df = main_df.join(
    DF_100_CRE_ETL_DISPERSION_VIV,
    (main_df["FCN_ID_TIPO_SUBCTA"] == DF_100_CRE_ETL_DISPERSION_VIV["FCN_ID_TIPO_SUBCTA"]) &
    (main_df["FCN_ID_SIEFORE"] == DF_100_CRE_ETL_DISPERSION_VIV["FCN_ID_SIEFORE"]) &
    (main_df["FTN_NUM_CTA_INVDUAL"] == DF_100_CRE_ETL_DISPERSION_VIV["FTN_NUM_CTA_INVDUAL"]) &
    (main_df["FTC_TABLA_NCI_MOV"] == DF_100_CRE_ETL_DISPERSION_VIV["FTC_TABLA_NCI_MOV"]) &
    (main_df["FTN_NO_LINEA"] == DF_100_CRE_ETL_DISPERSION_VIV["FTN_NO_LINEA"]) &
    (main_df["FFN_ID_CONCEPTO_MOV"] == DF_100_CRE_ETL_DISPERSION_VIV["FFN_ID_CONCEPTO_MOV"]),
    "left"
).select(main_df["*"], DF_100_CRE_ETL_DISPERSION_VIV["FTF_MONTO_ACCIONES"])

# unpersist the cache
DF_100_CRE_ETL_DISPERSION_VIV.unpersist

# eliminar los df
del DF_100_CRE_ETL_DISPERSION_VIV

if debug:
    display(main_df)

# COMMAND ----------

display(main_df.filter(col("FFN_ID_CONCEPTO_MOV")==225))

# COMMAND ----------

# display(main_df.filter(
#     (main_df["FFN_ID_CONCEPTO_MOV"] == 225) | 
#     (main_df["FFN_ID_CONCEPTO_MOV"] == 227)
# ))
# main_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Guardamos df_main en una ~~vista global~~, llamaremos a la vista `temp_dispersion_mov_01_{global_params['sr_folio']}`

# COMMAND ----------

# Save main_df to a global view named temp_dispersion_01_{global_params['sr_folio']}
view_name = f"temp_dispersion_mov_01_{global_params['SR_FOLIO']}"

spark.sql(f"DROP TABLE IF EXISTS {global_confs['catalog_name']}.{global_confs['schema_name']}.{view_name}")
main_df.write.format("delta").mode("overwrite").saveAsTable(
    f"{global_confs['catalog_name']}.{global_confs['schema_name']}.{view_name}"
)

# main_df.createOrReplaceGlobalTempView(view_name)

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
main_df.unpersist()

# Eliminar DataFrames para liberar memoria
del main_df

# Recolector de basura para liberar recursos inmediatamente
import gc
gc.collect()
