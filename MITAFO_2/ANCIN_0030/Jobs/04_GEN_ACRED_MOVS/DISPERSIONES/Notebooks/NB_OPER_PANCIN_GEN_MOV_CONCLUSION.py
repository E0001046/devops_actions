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
# MAGIC # Variables globales
# MAGIC prod = True
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
# MAGIC global_confs = {}
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
# MAGIC         "SR_ETAPA": "",
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
# MAGIC     # if any(not value for value in global_params.values()):
# MAGIC     #     logger.error("Valores de entrada vacíos o nulos")
# MAGIC     #     global_params["status"] = "0"
# MAGIC     # else:
# MAGIC     #     global_params["status"] = "1"
# MAGIC
# MAGIC     global_params["status"] = "1"
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
# MAGIC
# MAGIC # Asigna el manejador de excepciones al hook global de sys
# MAGIC sys.excepthook = global_exception_handler
# MAGIC
# MAGIC
# MAGIC if __name__ == "__main__":
# MAGIC     input_parameters = dbutils.widgets.getAll().items()
# MAGIC     # Inicialización de variables
# MAGIC     webhook_url, channel, failed_task = conf_init_values(
# MAGIC         config_files["general"], process_name, "GMV"
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
# MAGIC
# MAGIC     # Seteamos el valor de debug
# MAGIC     debug = conf_values[1]
# MAGIC     debug = debug.lower() == "true"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enviar notificacion al webhook de que el proceso finalizo de forma correcta

# COMMAND ----------

# Habilitar esta linea cuando halla webhook para GENERACION DE MOVIENTOS Y REEMPLAZAR TEMP_PROCESS EN LA CELDA 1
notification_raised(webhook_url, 0, "DONE", source, input_parameters)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean data

# COMMAND ----------

# Lista de nombres de tablas temporales basadas en 'SR_FOLIO'
temp_tables = [
    f"temp_dispersion_mov_01_{global_params['SR_FOLIO']}",
]

# Iterar sobre la lista de tablas temporales y eliminar cada una
for table_name in temp_tables:
    full_table_name = f"{global_confs['catalog_name']}.{global_confs['schema_name']}.{table_name}"
    try:
        spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
        print(f"Tabla {full_table_name} eliminada exitosamente.")
    except Exception as e:
        print(f"Error al eliminar la tabla {full_table_name}: {str(e)}")

# Forzar la recolección de basura
import gc
gc.collect()

print("Proceso de eliminación de tablas y liberación de memoria completado.")
