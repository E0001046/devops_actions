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
# MAGIC debug = False  # Para desactivar celdas
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
# MAGIC     if global_params["SR_TIPO_MOV"] == "ABONO":
# MAGIC         global_params["SR_FCN_ID_TIPO_MOV"] = 181
# MAGIC     else:
# MAGIC         global_params["SR_FCN_ID_TIPO_MOV"] = 180
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
# MAGIC         "table_015",
# MAGIC         "table_016",
# MAGIC         "table_017",
# MAGIC         "table_018",
# MAGIC         "table_019",
# MAGIC         "catalog_name",
# MAGIC         "schema_name",
# MAGIC
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

query_statement = "017"

table_name_001 = f"{global_confs['catalog_name']}.{global_confs['schema_name']}.TEMP_DOIMSS_ACR_01_{global_params['SR_FOLIO']}"

params = [
    table_name_001,
]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == "0":
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

df = spark.sql(statement)

if debug:
    display(df)

# COMMAND ----------

try:
    table_name_001 = f"{global_confs['conn_schema_002']}.{global_confs['table_015']}"
    mode = "APPEND"
    failed_task = write_into_table(
        conn_name_ora,
        df,
        mode,
        table_name_001,
        conn_options,
        conn_additional_options,
        conn_user,
        conn_key,
    )

    if failed_task == "0":
        logger.error("Please review log messages")
        notification_raised(webhook_url, -1, message, source, input_parameters)
        raise Exception("An error raised")
except Exception as e:
    logger.error(e)

# COMMAND ----------

# Liberar la caché del DataFrame si se usó cache
df.unpersist()

# Eliminar DataFrames para liberar memoria
del df

# Recolector de basura para liberar recursos inmediatamente
import gc
gc.collect()
