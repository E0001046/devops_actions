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
# MAGIC         "CX_CRE_ESQUEMA": "CIERREN_ETL",
# MAGIC         "TL_CRE_DISPERSION": "TTSISGRAL_ETL_DISPERSION",
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
# MAGIC         "table_001",
# MAGIC         "table_002",
# MAGIC         "table_003",
# MAGIC         "table_004",
# MAGIC         "table_005",
# MAGIC         "table_006",
# MAGIC         "table_007",
# MAGIC         "table_008",
# MAGIC         "table_009",
# MAGIC         "table_010",
# MAGIC         "table_011",
# MAGIC         "table_012",
# MAGIC         "table_013",
# MAGIC         "external_location",
# MAGIC         "err_repo_path",
# MAGIC         "output_file_name_001",
# MAGIC         "sep",
# MAGIC         "header",
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
# MAGIC def fix_created_file(file_name):
# MAGIC     try:
# MAGIC         file_name_tmp = dbutils.fs.ls(file_name + "_TEMP")
# MAGIC         file_name_new = list(filter(lambda x: x[0].endswith("csv"), file_name_tmp))
# MAGIC         dbutils.fs.mv(file_name_new[0][0], file_name)
# MAGIC         dbutils.fs.rm(file_name + "_TEMP", recurse=True)
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: " + str(inspect.stack()[0][3]))
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC         return "0"
# MAGIC     return "1"
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
# MAGIC ### Hacemos un query que tiene multiples extracciones y transformaciones en OCI

# COMMAND ----------

query_statement = "011"
table_name_001 = f"{global_confs['conn_schema_001']}.{global_confs['table_002']}" # CIERREN_ETL.TTSISGRAL_ETL_MOVIMIENTOS
table_name_002 = f"{global_confs['conn_schema_001']}.{global_confs['table_012']}" # CIERREN_ETL.TTSISGRAL_ETL_VAL_IDENT_CTE
table_name_003 = f"{global_confs['conn_schema_002']}.{global_confs['table_009']}" # CIERREN_ETL.TCAFOGRAL_VALOR_ACCION
table_name_004 = f"{global_confs['conn_schema_002']}.{global_confs['table_013']}" # CIERREN_ETL.TCCRXGRAL_CAT_CATALOGO

params = [
    table_name_001,
    table_name_002,
    table_name_003,
    table_name_004,
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
# MAGIC ### Generamos un .csv

# COMMAND ----------

from datetime import datetime
import pytz

header = True

full_file_name = (
    global_confs["external_location"]
    + global_confs["err_repo_path"]
    + "/"
    + global_params["SR_FOLIO"]
    + "_"
    + global_confs["output_file_name_001"]
)

try:
    # Guarda temporalmente el DataFrame en formato CSV
    df.write.format("csv").mode("overwrite").option("header", header).save(
        full_file_name + "_TEMP"
    )

    # Lee el archivo temporal en un nuevo DataFrame
    dataframe = spark.read.option("header", header).csv(full_file_name + "_TEMP")

    # Guarda el DataFrame en un solo archivo CSV
    dataframe.coalesce(1).write.format("csv").mode("overwrite").option(
        "header", header
    ).save(full_file_name)
except Exception as e:
    # Registra el error en el logger
    logger.error("An error was raised: " + str(e))
    # Envía una notificación de error
    notification_raised(webhook_url, -1, message, source, input_parameters)
    # Lanza una excepción para finalizar el proceso
    raise Exception("Process ends")

# COMMAND ----------

try:
    # Lee el archivo CSV en un DataFrame, con la opción de encabezado según la variable 'header'
    dfFile = spark.read.option('header', header).csv(full_file_name)
    
    # Escribe el DataFrame en un solo archivo CSV, con la opción de encabezado según la variable 'header'
    dfFile.coalesce(1).write.format('csv').mode('overwrite').option('header', header).save(full_file_name)
except Exception as e:
    # Registra el error en el logger
    logger.error("An error was raised: " + str(e))
    
    # Envía una notificación de error
    notification_raised(webhook_url, -1, message, source, input_parameters)
    
    # Lanza una excepción para finalizar el proceso
    raise Exception("Process ends")

# COMMAND ----------

try:
    # Define el nombre del archivo temporal auxiliar
    full_file_name_aux = full_file_name + '_TEMP'
    
    # Elimina el archivo temporal auxiliar si existe
    dbutils.fs.rm(full_file_name_aux, recurse=True)

    # Lista los archivos en el directorio del archivo original
    file_name_tmp = dbutils.fs.ls(full_file_name)
    
    # Filtra la lista de archivos para encontrar el archivo CSV
    file_name_new = list(filter(lambda x: x[0].endswith('csv'), file_name_tmp))

    # Copia el archivo CSV encontrado al archivo temporal auxiliar
    dbutils.fs.cp(file_name_new[0][0], full_file_name_aux)
    
    # Elimina el archivo original
    dbutils.fs.rm(full_file_name, recurse=True)
    
    # Copia el archivo temporal auxiliar al nombre del archivo original
    dbutils.fs.cp(full_file_name_aux, full_file_name)
    
    # Elimina el archivo temporal auxiliar
    dbutils.fs.rm(full_file_name_aux, recurse=True)
except Exception as e:
    # Registra el error en el logger
    logger.error("An error was raised: " + str(e))
    
    # Envía una notificación de error
    notification_raised(webhook_url, -1, message, source, input_parameters)
    
    # Lanza una excepción para finalizar el proceso
    raise Exception("Process ends")

# COMMAND ----------

# # Copia el archivo generado al volumen asignado para poder descargarlo en el equipo local
# dbutils.fs.cp(
#    f"abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/OUT/{global_params['SR_FOLIO']}_INSUFICIENCIA.csv", 
#    f"/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/{global_params['SR_FOLIO']}_INSUFICIENCIA.csv"
# )

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
df.unpersist()

# Eliminar DataFrames para liberar memoria
del df

# Recolector de basura para liberar recursos inmediatamente
import gc
gc.collect()
