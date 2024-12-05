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
# MAGIC debug = True  # Para desactivar celdas
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
# MAGIC         "SR_SUBPROCESO": "",
# MAGIC         "SR_PROCESO": "",
# MAGIC         "SR_USUARIO": "",
# MAGIC         "SR_SUBETAPA": "",
# MAGIC         "SR_FCC_USU_ACT": "",
# MAGIC         "SR_FNC_USU_CRE": "",
# MAGIC         "SR_INSTANCIA_PROCESO": "",
# MAGIC         "SR_ETAPA": "",
# MAGIC         "SR_FOLIO": "",
# MAGIC         "SR_TIPO_MOV": "",
# MAGIC         "SR_ID_SNAPSHOT": "",
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
# MAGIC     if any(not value for value in global_params.values()):
# MAGIC         logger.error("Valores de entrada vacíos o nulos")
# MAGIC         global_params["status"] = "0"
# MAGIC     else:
# MAGIC         global_params["status"] = "1"
# MAGIC
# MAGIC     # Calculo de SR_FCN_ID_TIPO_MOV
# MAGIC     global_params["SR_FCN_ID_TIPO_MOV"] = 181
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
# MAGIC         "table_014",
# MAGIC         "table_011",
# MAGIC         "table_020",
# MAGIC         "table_021",
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
# MAGIC
# MAGIC if __name__ == "__main__":
# MAGIC     # Inicialización de variables
# MAGIC     process_name = "root"
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
# MAGIC
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
# MAGIC ### Hacemos esta consulta en OCI

# COMMAND ----------

query_statement = "025"
table_name_001 = f"{global_confs['conn_schema_001']}.{global_confs['table_020']}" #CIERREN_ETL.TLSISGRAL_ETL_CLIENTE_SUA

params = [
    table_name_001,
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

DF_100_TLSISGRAL_ETL_CLIENTE_SUA, failed_task = query_table(
    conn_name_ora, spark, formatted_statement, conn_options, conn_user, conn_key
)

# Inserto df al cache
DF_100_TLSISGRAL_ETL_CLIENTE_SUA.cache()

if failed_task == "0":
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")

if debug:
    display(DF_100_TLSISGRAL_ETL_CLIENTE_SUA)

# COMMAND ----------

query_statement = "026"
tabale_name_001 = f"{global_confs['conn_schema_001']}.{global_confs['table_002']}" #CIERREN_ETL.TTSISGRAL_ETL_MOVIMIENTOS

params = [
    tabale_name_001,
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

DF_200_TTSISGRAL_ETL_MOVIMIENTOS, failed_task = query_table(
    conn_name_ora, spark, formatted_statement, conn_options, conn_user, conn_key
)

# Inserto df al cache
DF_200_TTSISGRAL_ETL_MOVIMIENTOS.cache()

if failed_task == "0":
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")

if debug:
    display(DF_200_TTSISGRAL_ETL_MOVIMIENTOS)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Removemos duplicados de DF_100_TLSISGRAL_ETL_CLIENTE_SUA por los campos:
# MAGIC Keys:
# MAGIC
# MAGIC - FNN_ID_REFERENCIA
# MAGIC - FTC_FOLIO
# MAGIC - FNC_TIP_REG

# COMMAND ----------

# Remove duplicates from DF_100_TLSISGRAL_ETL_CLIENTE_SUA based on specified keys
DF_100_TLSISGRAL_ETL_CLIENTE_SUA = DF_100_TLSISGRAL_ETL_CLIENTE_SUA.dropDuplicates(["FNN_ID_REFERENCIA", "FTC_FOLIO", "FNC_TIPREG"])

# Display the DataFrame to verify the duplicates are removed
if debug:
    display(DF_100_TLSISGRAL_ETL_CLIENTE_SUA)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hacemos un inner join entre `DF_100_TLSISGRAL_ETL_CLIENTE_SUA` y `DF_200_TTSISGRAL_ETL_MOVIMIENTOS` por las keys `FNN_ID_REFERENCIA` Y `FTC_FOLIO`.
# MAGIC La finalidad es maperar la siguiente key (`FTN_NUM_CTA_INVDUAL`) de `DF_200_TTSISGRAL_ETL_MOVIMIENTOS`

# COMMAND ----------

# Perform an inner join between DF_100_TLSISGRAL_ETL_CLIENTE_SUA and DF_200_TTSISGRAL_ETL_MOVIMIENTOS
# on the keys FNN_ID_REFERENCIA and FTC_FOLIO to map the key FTN_NUM_CTA_INVDUAL from DF_200_TTSISGRAL_ETL_MOVIMIENTOS

joined_df = DF_100_TLSISGRAL_ETL_CLIENTE_SUA.join(
    DF_200_TTSISGRAL_ETL_MOVIMIENTOS,
    on=["FNN_ID_REFERENCIA", "FTC_FOLIO"],
    how="inner"
).select(
    DF_100_TLSISGRAL_ETL_CLIENTE_SUA["*"],
    DF_200_TTSISGRAL_ETL_MOVIMIENTOS["FTN_NUM_CTA_INVDUAL"]
)

DF_100_TLSISGRAL_ETL_CLIENTE_SUA.unpersist()
DF_200_TTSISGRAL_ETL_MOVIMIENTOS.unpersist()
del DF_100_TLSISGRAL_ETL_CLIENTE_SUA, DF_200_TTSISGRAL_ETL_MOVIMIENTOS

joined_df.cache()

# Display the joined DataFrame
if debug:
    display(joined_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creamos estos campos:
# MAGIC - FNN_NUMREG = “”
# MAGIC - FNN_AFORE = “”
# MAGIC - FNN_DEVPAG = “”
# MAGIC - FND_FEH_CRE = current time stamp
# MAGIC - FNC_USU_CRE → CX_CRE_USUARIO.

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp, col

FNC_USU_CRE = global_params["SR_FNC_USU_CRE"]

df_joined = joined_df.withColumn("FNN_NUMREG", lit("")) \
                     .withColumn("FNN_AFORE", lit("")) \
                     .withColumn("FNN_DEVPAG", lit("")) \
                     .withColumn("FND_FEH_CRE", current_timestamp()) \
                     .withColumn("FNC_USU_CRE", lit(FNC_USU_CRE))

display(df_joined)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insertamos en `CIERREN.TNAFORECA_SUA`.

# COMMAND ----------

# Lista de columnas que deseas seleccionar
columns_to_select = [
    "FNN_ID_REFERENCIA",
    "FTC_FOLIO",
    "FTN_NUM_CTA_INVDUAL",
    "FTN_NSS",
    "FTC_CURP",
    "FTC_NOMBRE",
    "FNC_PERIODO_PAGO_PATRON",
    "FND_FECHA_PAGO",
    "FND_FECHA_VALOR_IMSS_ACV_VIV",
    "FNN_ULTIMO_SALARIO_INT_PER",
    "FNC_FOLIO_PAGO_SUA",
    "FNC_REG_PATRONAL_IMSS",
    "FNC_RFC_PATRON",
    "FNN_DIAS_COTZDOS_BIMESTRE",
    "FNN_DIAS_INCAP_BIMESTRE",
    "FNN_DIAS_AUSENT_BIMESTRE",
    "FNN_ID_VIV_GARANTIA",
    "FNN_APLIC_INT_VIVIENDA",
    "FNC_MOTIVO_LIBCION_APORT",
    "FND_FECHA_PAGO_CUOTA_GUB",
    "FND_FECHA_VALOR_RCV",
    "FNN_CVE_ENT_RECEP_PAGO",
    "FNN_NSS_ISSSTE",
    "FNC_DEPEND_CTRO_PAGO",
    "FNN_CTRO_PAGO_SAR",
    "FNN_CVE_RAMO",
    "FNC_CVE_PAGADURIA",
    "FNN_SUELDO_BASICO_COTIZ_RCV",
    "FNC_ID_TRABAJADOR_BONO",
    "FTN_INTERESES_VIV92",
    "FTN_INTERESES_VIV08",
    "FNC_TIPREG",
    "FTN_CONS_REG_LOTE",
    "FNN_SECLOT",
    "FND_FECTRA",
    "FNN_NUMREG",
    "FNN_AFORE",
    "FNN_DEVPAG",
    "FND_FEH_CRE",
    "FNC_USU_CRE",
    "FCN_ID_TIPO_APO"
]

# Seleccionar las columnas especificadas y reemplazar el DataFrame original
df_joined = df_joined.select(*columns_to_select)

if debug:
    display(df_joined)


# COMMAND ----------

mode = "APPEND"
table_name_001 = f"{global_confs['conn_schema_002']}.{global_confs['table_021']}"  # CIERREN.TNAFORECA_SUA

failed_task = write_into_table(
    conn_name_ora,
    df_joined,
    mode,
    table_name_001,
    conn_options,
    conn_additional_options,
    conn_user,
    conn_key,
)

failed_task = "1"

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
df_joined.unpersist()

# Eliminar DataFrames para liberar memoria
del df_joined

# Recolector de basura para liberar recursos inmediatamente
import gc
gc.collect()

# COMMAND ----------

# Habilitar esta linea cuando halla webhook para GENERACION DE MOVIENTOS Y REEMPLAZAR TEMP_PROCESS EN LA CELDA 1
notification_raised(webhook_url, 0, "DONE", source, input_parameters)

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {global_confs['catalog_name']}.{global_confs['schema_name']}.TEMP_DOIMSS_ACR_01_{global_params['SR_FOLIO']}")
