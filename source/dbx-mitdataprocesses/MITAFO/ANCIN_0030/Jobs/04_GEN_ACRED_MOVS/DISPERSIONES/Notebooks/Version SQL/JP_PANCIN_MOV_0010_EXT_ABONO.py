# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC %reload_ext autoreload
# MAGIC import configparser
# MAGIC import inspect
# MAGIC import json
# MAGIC import logging
# MAGIC import sys
# MAGIC
# MAGIC logging.basicConfig()
# MAGIC logger = logging.getLogger(__name__)
# MAGIC logger.setLevel(logging.DEBUG)
# MAGIC
# MAGIC ROOT_REPO = "/Workspace/Shared/MITAFO"
# MAGIC sys.path.append(ROOT_REPO + "/" + "CGRLS_0010/Notebooks")
# MAGIC
# MAGIC # TODO: CAMBIAR A CLASES E IMPORTAR OBJETO DE LA CLASE PARA EVITAR '*'
# MAGIC from NB_GRLS_DML_FUNCTIONS import *
# MAGIC from NB_GRLS_SIMPLE_FUNCTIONS import *
# MAGIC
# MAGIC
# MAGIC def input_values() -> tuple:
# MAGIC     """Obtiene los valores de los widgets de entrada.
# MAGIC
# MAGIC     Respuesta:
# MAGIC     ----------
# MAGIC         tuple: Una tupla que contiene los valores de los widgets de entrada y
# MAGIC         una bandera de estado.
# MAGIC     """
# MAGIC
# MAGIC     # Definir claves y valores predeterminados para los widgets
# MAGIC     widget_defaults = {
# MAGIC         "SR_FTC_FOLIO": "202311211301042007",
# MAGIC         "SR_FOLIO_REL": "302311211301042008",
# MAGIC         "SR_FEC_ACC": "2024-07-30",
# MAGIC         "SR_TIPO_MOV": "2",
# MAGIC         "SR_FTD_FEH_LIQUIDACION": "2024-07-30",
# MAGIC         "SR_FCC_USU_CRE": "DATABRICKS",
# MAGIC         "SR_FACTOR": "1",
# MAGIC         "SR_FLC_USU_REG": "DATABRICKS",
# MAGIC         "SR_ETAPA": "1",
# MAGIC     }
# MAGIC
# MAGIC     # Crear widgets con valores predeterminados
# MAGIC     for key, default_value in widget_defaults.items():
# MAGIC         dbutils.widgets.text(key, default_value)
# MAGIC
# MAGIC     try:
# MAGIC         # Recuperar los valores de los widgets
# MAGIC         values = {key: dbutils.widgets.get(key).strip() for key in widget_defaults}
# MAGIC
# MAGIC         # Verificar si alguno de los valores está vacío
# MAGIC         if any(not value for value in values.values()):
# MAGIC             logger.error("Function: input_values")
# MAGIC             logger.error("Some of the input values are empty or null")
# MAGIC             return tuple("0" for _ in widget_defaults) + ("0",)
# MAGIC
# MAGIC         # Todos los valores son válidos
# MAGIC         return tuple(values.values()) + ("1",)
# MAGIC
# MAGIC     except (ValueError, IOError) as error:
# MAGIC         logger.error("Function: input_values")
# MAGIC         logger.error("An error was raised: %s", str(error))
# MAGIC         return tuple("0" for _ in widget_defaults) + ("0",)
# MAGIC
# MAGIC
# MAGIC def conf_process_values(arg_config_file: str, arg_process_name: str) -> tuple:
# MAGIC     """Obtiene los valores de configuración del proceso.
# MAGIC
# MAGIC     Parámetros:
# MAGIC     -----------
# MAGIC         arg_CONFIG_FILE (str): Ruta del archivo de configuración.
# MAGIC         arf_PROCESS_NAME (str): Nombre del proceso.
# MAGIC
# MAGIC     Respuesta:
# MAGIC     ----------
# MAGIC         tuple: Una tupla que contiene los valores de configuración del proceso
# MAGIC         y una bandera de estado.
# MAGIC     """
# MAGIC
# MAGIC     try:
# MAGIC         config = configparser.ConfigParser()
# MAGIC         config.read(arg_config_file)
# MAGIC         result_sql_conf_file = config.get(arg_process_name, "sql_conf_file")
# MAGIC         result_conn_schema_001 = config.get(
# MAGIC             arg_process_name,
# MAGIC             "conn_schema_001",
# MAGIC         )
# MAGIC         result_conn_schema_002 = config.get(
# MAGIC             arg_process_name,
# MAGIC             "conn_schema_002",
# MAGIC         )
# MAGIC         result_table_001 = config.get(arg_process_name, "table_001")
# MAGIC         result_table_002 = config.get(arg_process_name, "table_002")
# MAGIC         result_table_003 = config.get(arg_process_name, "table_003")
# MAGIC         result_table_004 = config.get(arg_process_name, "table_004")
# MAGIC         result_table_005 = config.get(arg_process_name, "table_005")
# MAGIC         result_table_006 = config.get(arg_process_name, "table_006")
# MAGIC         result_table_007 = config.get(arg_process_name, "table_007")
# MAGIC         result_table_008 = config.get(arg_process_name, "table_008")
# MAGIC         result_table_009 = config.get(arg_process_name, "table_009")
# MAGIC         result_table_010 = config.get(arg_process_name, "table_010")
# MAGIC     except (ValueError, IOError) as error:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: %s", str(error))
# MAGIC         return (
# MAGIC             "0",
# MAGIC             "0",
# MAGIC             "0",
# MAGIC             "0",
# MAGIC             "0",
# MAGIC             "0",
# MAGIC             "0",
# MAGIC             "0",
# MAGIC             "0",
# MAGIC             "0",
# MAGIC             "0",
# MAGIC             "0",
# MAGIC             "0",
# MAGIC             "0",
# MAGIC         )
# MAGIC     return (
# MAGIC         result_sql_conf_file,
# MAGIC         result_conn_schema_001,
# MAGIC         result_conn_schema_002,
# MAGIC         result_table_001,
# MAGIC         result_table_002,
# MAGIC         result_table_003,
# MAGIC         result_table_004,
# MAGIC         result_table_005,
# MAGIC         result_table_006,
# MAGIC         result_table_007,
# MAGIC         result_table_008,
# MAGIC         result_table_009,
# MAGIC         result_table_010,
# MAGIC         "1",
# MAGIC     )
# MAGIC
# MAGIC
# MAGIC if __name__ == "__main__":
# MAGIC
# MAGIC     notebook_name = (
# MAGIC         dbutils.notebook.entry_point.getDbutils()
# MAGIC         .notebook()
# MAGIC         .getContext()
# MAGIC         .notebookPath()
# MAGIC         .get()
# MAGIC     )
# MAGIC
# MAGIC     MESSAGE = "NB Error: " + notebook_name
# MAGIC     SOURCE = "ETL"
# MAGIC     CONFIG_FILE = ROOT_REPO + "/" + "CGRLS_0010/Conf/CF_GRLS_PROCESS.py.properties"
# MAGIC     CONFIG_CONN_FILE = ROOT_REPO + "/" + "CGRLS_0010/Conf/CF_GRLS_CONN.py.properties"
# MAGIC
# MAGIC     # TODO: CONTROLAR POR VARIABLE DE ENTORNO
# MAGIC     CONFIG_PROCESS_FILE = (
# MAGIC         ROOT_REPO
# MAGIC         + "/"
# MAGIC         + "ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/DISPERSIONES/Conf/CF_PART_PROC.py.properties"
# MAGIC     )
# MAGIC
# MAGIC     (
# MAGIC         SR_FTC_FOLIO,
# MAGIC         SR_FOLIO_REL,
# MAGIC         SR_FEC_ACC,
# MAGIC         SR_TIPO_MOV,
# MAGIC         SR_FTD_FEH_LIQUIDACION,
# MAGIC         SR_FCC_USU_CRE,
# MAGIC         SR_FACTOR,
# MAGIC         SR_FLC_USU_REG,
# MAGIC         SR_ETAPA,
# MAGIC         failed_task,
# MAGIC     ) = input_values()
# MAGIC
# MAGIC     if failed_task == "0":
# MAGIC         logger.error("Please review log messages")
# MAGIC         # notification_raised(webhook_url, -1, MESSAGE, SOURCE, input_parameters)
# MAGIC         raise ValueError("An error ocurred, check out log messages")
# MAGIC
# MAGIC     input_parameters = dbutils.widgets.getAll().items()
# MAGIC
# MAGIC     PROCESS_NAME = "root"
# MAGIC     webhook_url, channel, failed_task = conf_init_values(CONFIG_FILE, PROCESS_NAME, "TEMP_PROCESS",)
# MAGIC     if failed_task == "0":
# MAGIC         logger.error("Please review log MESSAGEs")
# MAGIC         notification_raised(webhook_url, -1, MESSAGE, SOURCE, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     PROCESS_NAME = "root"
# MAGIC     (
# MAGIC         sql_conf_file,
# MAGIC         conn_schema_001,
# MAGIC         conn_schema_002,
# MAGIC         table_001,
# MAGIC         table_002,
# MAGIC         table_003,
# MAGIC         table_004,
# MAGIC         table_005,
# MAGIC         table_006,
# MAGIC         table_007,
# MAGIC         table_008,
# MAGIC         table_009,
# MAGIC         table_010,
# MAGIC         failed_task,
# MAGIC     ) = conf_process_values(CONFIG_PROCESS_FILE, PROCESS_NAME)
# MAGIC     if failed_task == "0":
# MAGIC         logger.error("Please review log MESSAGEs")
# MAGIC         notification_raised(webhook_url, -1, MESSAGE, SOURCE, input_parameters)
# MAGIC         raise ValueError("Process ends")
# MAGIC
# MAGIC     CONN_NAME_ORA = "jdbc_oracle"
# MAGIC     CONN_OPTIONS, CONN_ADDITIONAL_OPTIONS, CON_USER, CONN_KEY, CONN_URL, failed_task = (
# MAGIC         conf_conn_values(CONFIG_CONN_FILE, CONN_NAME_ORA)
# MAGIC     )
# MAGIC     if failed_task == "0":
# MAGIC         logger.error("Please review log MESSAGEs")
# MAGIC         notification_raised(webhook_url, -1, MESSAGE, SOURCE, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     # TODO: CONTROLAR POR VARIABLE DE ENTORNO
# MAGIC     sql_conf_file = (
# MAGIC         ROOT_REPO
# MAGIC         + "/"
# MAGIC         + "ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/DISPERSIONES/JSON/"
# MAGIC         + sql_conf_file
# MAGIC     )
# MAGIC

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

table_name_001 = conn_schema_001 + "." + table_001
table_name_002 = conn_schema_002 + "." + table_002
table_name_003 = conn_schema_002 + "." + table_003
table_name_004 = conn_schema_002 + "." + table_004
table_name_005 = conn_schema_002 + "." + table_005

query_statement = "001"

params = [
    SR_FTC_FOLIO,
    SR_FOLIO_REL,
    table_name_001,
    table_name_002,
    table_name_003,
    table_name_004,
    SR_FEC_ACC,
    table_name_005,
]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == "0":
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

# COMMAND ----------

df, failed_task = query_table(
    CONN_NAME_ORA, spark, statement, CONN_OPTIONS, CON_USER, CONN_KEY
)

# COMMAND ----------

df.createOrReplaceTempView("TEMP_DISPERSION_MOV_01")
spark.sql("SELECT * FROM TEMP_DISPERSION_MOV_01").show()


# COMMAND ----------

query_statement = "002"

TEMP_VIEW = "TEMP_DISPERSION_MOV_01"

params = [
    TEMP_VIEW,
    SR_FTC_FOLIO,
    SR_TIPO_MOV,
    SR_FTD_FEH_LIQUIDACION,
    SR_FCC_USU_CRE,
]

statement, failed_task = getting_statement(conf_values, query_statement, params)

# COMMAND ----------

statement

# COMMAND ----------

if failed_task == "0":
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

# COMMAND ----------

df = spark.sql(statement)

# COMMAND ----------

df.createOrReplaceTempView("TEMP_DISPERSION_MOV_02")
spark.sql("SELECT * FROM TEMP_DISPERSION_MOV_02").show()

# COMMAND ----------

table_name_001 = conn_schema_002 + "." + table_006
table_name_002 = conn_schema_001 + "." + table_007

query_statement = "003"

params = [table_name_001, table_name_002]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == "0":
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

# COMMAND ----------

df, failed_task = query_table(
    CONN_NAME_ORA, spark, statement, CONN_OPTIONS, CON_USER, CONN_KEY
)

# COMMAND ----------

df.createOrReplaceTempView("TEMP_DISPERSION_MOV_AUX_01")
spark.sql("SELECT * FROM TEMP_DISPERSION_MOV_AUX_01").show()

# COMMAND ----------

query_statement = "004"

TEMP_VIEW_1 = "TEMP_DISPERSION_MOV_02"
TEMP_VIEW_2 = "TEMP_DISPERSION_MOV_AUX_01"

params = [
    TEMP_VIEW_1,
    TEMP_VIEW_2,
]

statement, failed_task = getting_statement(conf_values, query_statement, params)

# COMMAND ----------

statement

# COMMAND ----------

if failed_task == "0":
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

# COMMAND ----------

df = spark.sql(statement)

# COMMAND ----------

# TEMP_VIEW = f"TEMP_DISPERSION_MOV_{SR_FTC_FOLIO}"
# df.createOrReplaceGlobalTempView(TEMP_VIEW)
# spark.sql(f"SELECT * FROM global_temp.{TEMP_VIEW}").show()

# COMMAND ----------

catalog_name = "dbx_mit_dev_1udbvf_workspace"
schema_name = "default"
table_name = f"TEMP_DISPERSION_MOV_{SR_FTC_FOLIO}"

df.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog_name}.{schema_name}.{table_name}"
)
spark.sql(f"SELECT * FROM {catalog_name}.{schema_name}.{table_name}").show()
