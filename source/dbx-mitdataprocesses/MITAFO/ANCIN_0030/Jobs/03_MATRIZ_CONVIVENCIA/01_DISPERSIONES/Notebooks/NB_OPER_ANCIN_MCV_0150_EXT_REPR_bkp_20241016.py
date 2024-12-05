# Databricks notebook source
# MAGIC  %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC %reload_ext autoreload
# MAGIC
# MAGIC import sys
# MAGIC import inspect
# MAGIC import configparser
# MAGIC import json
# MAGIC import logging
# MAGIC import uuid
# MAGIC
# MAGIC from pyspark.sql.types import (
# MAGIC     StructType,
# MAGIC     StructField,
# MAGIC     StringType,
# MAGIC     DoubleType,
# MAGIC     IntegerType,
# MAGIC )
# MAGIC from pyspark.sql.functions import length, lit
# MAGIC
# MAGIC
# MAGIC def input_values():
# MAGIC     """Retrieve input values from widgets.
# MAGIC     Returns:
# MAGIC         tuple: A tuple containing the input values and a status flag.
# MAGIC     """
# MAGIC     try:
# MAGIC         # Define widgets de texto en Databricks para capturar valores de entrada.
# MAGIC         dbutils.widgets.text("sr_tipo_mov", "")
# MAGIC         dbutils.widgets.text("sr_proceso", "")
# MAGIC         dbutils.widgets.text("sr_subproceso", "")
# MAGIC         dbutils.widgets.text("sr_origen_arc", "")
# MAGIC         dbutils.widgets.text("sr_dt_org_arc", "")
# MAGIC         dbutils.widgets.text("sr_fecha_acc", "")
# MAGIC         dbutils.widgets.text("sr_subetapa", "")
# MAGIC         dbutils.widgets.text("sr_folio", "")
# MAGIC         dbutils.widgets.text("sr_folio_rel", "")
# MAGIC         dbutils.widgets.text("sr_reproceso", "")
# MAGIC         dbutils.widgets.text("sr_aeim", "")
# MAGIC         dbutils.widgets.text("sr_instancia_proceso", "")
# MAGIC         dbutils.widgets.text("sr_usuario", "")
# MAGIC         dbutils.widgets.text("sr_etapa", "")
# MAGIC         dbutils.widgets.text("sr_id_snapshot", "")
# MAGIC
# MAGIC         # Recupera los valores de los widgets definidos anteriormente.
# MAGIC         sr_tipo_mov = dbutils.widgets.get("sr_tipo_mov")
# MAGIC         sr_proceso = dbutils.widgets.get("sr_proceso")
# MAGIC         sr_subproceso = dbutils.widgets.get("sr_subproceso")
# MAGIC         sr_origen_arc = dbutils.widgets.get("sr_origen_arc")
# MAGIC         sr_dt_org_arc = dbutils.widgets.get("sr_dt_org_arc")
# MAGIC         sr_fecha_acc = dbutils.widgets.get("sr_fecha_acc")
# MAGIC         sr_subetapa = dbutils.widgets.get("sr_subetapa")
# MAGIC         sr_folio = dbutils.widgets.get("sr_folio")
# MAGIC         sr_folio_rel = dbutils.widgets.get("sr_folio_rel")
# MAGIC         sr_reproceso = dbutils.widgets.get("sr_reproceso")
# MAGIC         sr_aeim = dbutils.widgets.get("sr_aeim")
# MAGIC         sr_instancia_proceso = dbutils.widgets.get("sr_instancia_proceso")
# MAGIC         sr_usuario = dbutils.widgets.get("sr_usuario")
# MAGIC         sr_etapa = dbutils.widgets.get("sr_etapa")
# MAGIC         sr_id_snapshot = dbutils.widgets.get("sr_id_snapshot")
# MAGIC
# MAGIC         # Verifica si alguno de los valores recuperados está vacío o es nulo
# MAGIC         # Si algún valor está vacío, registra un error y retorna una tupla de ceros
# MAGIC         if any(
# MAGIC             len(str(value).strip()) == 0
# MAGIC             for value in [
# MAGIC                 sr_tipo_mov,
# MAGIC                 sr_proceso,
# MAGIC                 sr_subproceso,
# MAGIC                 sr_origen_arc,
# MAGIC                 sr_dt_org_arc,
# MAGIC                 sr_fecha_acc,
# MAGIC                 sr_subetapa,
# MAGIC                 sr_folio,
# MAGIC                 sr_folio_rel,
# MAGIC                 sr_reproceso,
# MAGIC                 sr_aeim,
# MAGIC                 sr_instancia_proceso,
# MAGIC                 sr_usuario,
# MAGIC                 sr_etapa,
# MAGIC                 sr_id_snapshot,
# MAGIC             ]
# MAGIC         ):
# MAGIC             logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC             logger.error("Some of the input values are empty or null")
# MAGIC             return "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"
# MAGIC     # Captura cualquier excepción que ocurra durante la ejecución del bloque try
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: %s", str(e))
# MAGIC         return "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"
# MAGIC     # Si no hay errores, retorna los valores recuperados y un indicador de éxito ('1')
# MAGIC     return (
# MAGIC         sr_tipo_mov,
# MAGIC         sr_proceso,
# MAGIC         sr_subproceso,
# MAGIC         sr_origen_arc,
# MAGIC         sr_dt_org_arc,
# MAGIC         sr_fecha_acc,
# MAGIC         sr_subetapa,
# MAGIC         sr_folio,
# MAGIC         sr_folio_rel,
# MAGIC         sr_reproceso,
# MAGIC         sr_aeim,
# MAGIC         sr_instancia_proceso,
# MAGIC         sr_usuario,
# MAGIC         sr_etapa,
# MAGIC         sr_id_snapshot,
# MAGIC         "1",
# MAGIC     )
# MAGIC
# MAGIC
# MAGIC def conf_process_values(config_file, process_name):
# MAGIC     """Retrieve process configuration values from a config file.
# MAGIC     Args:
# MAGIC         config_file (str): Path to the configuration file.
# MAGIC         process_name (str): Name of the process.
# MAGIC     Returns:
# MAGIC         tuple: A tuple containing the configuration values and a status flag.
# MAGIC     """
# MAGIC     try:
# MAGIC         ## Inicializa un objeto ConfigParser y lee el archivo de configuración especificado por config_file
# MAGIC         config = configparser.ConfigParser()
# MAGIC         config.read(config_file)
# MAGIC         # Configuración de subprocesos
# MAGIC         # Recupera los valores de configuración específicos para el proceso indicado por process_name
# MAGIC         sql_conf_file = config.get(process_name, "sql_conf_file_reproceso_abono")
# MAGIC         conn_schema_001 = config.get(process_name, "conn_schema_001")
# MAGIC         table_001 = config.get(process_name, "table_001")
# MAGIC         table_002 = config.get(process_name, "table_002")
# MAGIC         conn_schema_002 = config.get(process_name, "conn_schema_002")
# MAGIC         table_003 = config.get(process_name, "table_003")
# MAGIC         # Unity
# MAGIC         debug = config.get(process_name, "debug")
# MAGIC         debug = debug.lower() == "true"
# MAGIC         catalog_name = config.get(process_name, "catalog_name")
# MAGIC         schema_name = config.get(process_name, "schema_name")
# MAGIC     except Exception as e:
# MAGIC         # Captura cualquier excepción que ocurra durante la ejecución del bloque try.
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: " + str(e))
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
# MAGIC         )
# MAGIC     # Return the retrieved configuration values and a success flag
# MAGIC     return (
# MAGIC         sql_conf_file,
# MAGIC         conn_schema_001,
# MAGIC         table_001,
# MAGIC         table_002,
# MAGIC         conn_schema_002,
# MAGIC         table_003,
# MAGIC         debug,
# MAGIC         # unity
# MAGIC         catalog_name,
# MAGIC         schema_name,
# MAGIC         "1",
# MAGIC     )
# MAGIC
# MAGIC
# MAGIC # El código principal se ejecuta cuando el script se ejecuta directamente (no cuando se importa como un módulo).
# MAGIC if __name__ == "__main__":
# MAGIC     # Configura el logger para registrar mensajes de depuración y errores.
# MAGIC     logging.basicConfig()
# MAGIC     logger = logging.getLogger(__name__)
# MAGIC     logger.setLevel(logging.DEBUG)
# MAGIC     # Obtiene el nombre del notebook y define variables para mensajes de error y la ruta del repositorio raíz.
# MAGIC     notebook_name = (
# MAGIC         dbutils.notebook.entry_point.getDbutils()
# MAGIC         .notebook()
# MAGIC         .getContext()
# MAGIC         .notebookPath()
# MAGIC         .get()
# MAGIC     )
# MAGIC     message = "NB Error: " + notebook_name
# MAGIC     source = "ETL"
# MAGIC     root_repo = "/Workspace/Shared/MITAFO"
# MAGIC
# MAGIC     # Intenta importar funciones adicionales desde otros notebooks. Si ocurre un error, lo registra.
# MAGIC     try:
# MAGIC         sys.path.append(root_repo + "/" + "CGRLS_0010/Notebooks")
# MAGIC         from NB_GRLS_DML_FUNCTIONS import *
# MAGIC         from NB_GRLS_SIMPLE_FUNCTIONS import *
# MAGIC     except Exception as e:
# MAGIC         logger.error("Error at the beggining of the process")
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC
# MAGIC     # Define las rutas de los archivos de configuración necesarios para el proceso.
# MAGIC     config_file = root_repo + "/" + "CGRLS_0010/Conf/CF_GRLS_PROCESS.py.properties"
# MAGIC     config_conn_file = root_repo + "/" + "CGRLS_0010/Conf/CF_GRLS_CONN.py.properties"
# MAGIC     config_process_file = (
# MAGIC         root_repo
# MAGIC         + "/"
# MAGIC         + "ANCIN_0030/Jobs/03_MATRIZ_CONVIVENCIA/01_DISPERSIONES/Conf/CF_PART_PROC.py.properties"
# MAGIC     )
# MAGIC
# MAGIC     # Llama a la función input_values para recuperar los valores de entrada. Si hay un error, registra el mensaje y lanza una excepción.
# MAGIC     (
# MAGIC         sr_tipo_mov,
# MAGIC         sr_proceso,
# MAGIC         sr_subproceso,
# MAGIC         sr_origen_arc,
# MAGIC         sr_dt_org_arc,
# MAGIC         sr_fecha_acc,
# MAGIC         sr_subetapa,
# MAGIC         sr_folio,
# MAGIC         sr_folio_rel,
# MAGIC         sr_reproceso,
# MAGIC         sr_aeim,
# MAGIC         sr_instancia_proceso,
# MAGIC         sr_usuario,
# MAGIC         sr_etapa,
# MAGIC         sr_id_snapshot,
# MAGIC         failed_task,
# MAGIC     ) = input_values()
# MAGIC     if failed_task == "0":
# MAGIC         logger.error("Please review log messages")
# MAGIC         # notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("An error ocurred, check out log messages")
# MAGIC
# MAGIC     # Obtiene todos los parámetros de entrada desde los widgets de Databricks.
# MAGIC     input_parameters = dbutils.widgets.getAll().items()
# MAGIC
# MAGIC     # Llama a la función conf_init_values para recuperar los valores de configuración inicial. Si hay un error, registra el mensaje, envía una notificación y lanza una excepción.
# MAGIC     process_name = "root"
# MAGIC     webhook_url, channel, failed_task = conf_init_values(
# MAGIC         config_file, process_name, "MCV"
# MAGIC     )
# MAGIC     if failed_task == "0":
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     # Llama a la función conf_process_values para recuperar los valores de configuración del proceso. Si hay un error, registra el mensaje, envía una notificación y lanza una excepción.
# MAGIC     process_name = "root"
# MAGIC     (
# MAGIC         sql_conf_file,
# MAGIC         conn_schema_001,
# MAGIC         table_001,
# MAGIC         table_002,
# MAGIC         conn_schema_002,
# MAGIC         table_003,
# MAGIC         debug,
# MAGIC         # unity
# MAGIC         catalog_name,
# MAGIC         schema_name,
# MAGIC         failed_task,
# MAGIC     ) = conf_process_values(config_process_file, process_name)
# MAGIC     if failed_task == "0":
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     # Llama a la función conf_conn_values para recuperar los valores de configuración de conexión. Si hay un error, registra el mensaje, envía una notificación y lanza una excepción.
# MAGIC     conn_name_ora = "jdbc_oracle"
# MAGIC     (
# MAGIC         conn_options,
# MAGIC         conn_aditional_options,
# MAGIC         conn_user,
# MAGIC         conn_key,
# MAGIC         conn_url,
# MAGIC         failed_task,
# MAGIC     ) = conf_conn_values(config_conn_file, conn_name_ora)
# MAGIC     if failed_task == "0":
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     # Definición de la ruta del archivo de configuración SQL
# MAGIC     sql_conf_file = (
# MAGIC         root_repo
# MAGIC         + "/"
# MAGIC         + "ANCIN_0030/Jobs/03_MATRIZ_CONVIVENCIA/01_DISPERSIONES/JSON/"
# MAGIC         + sql_conf_file
# MAGIC     )
# MAGIC
# MAGIC if sr_folio_rel.lower() == 'na':
# MAGIC     sr_folio_rel = ''

# COMMAND ----------

with open(sql_conf_file) as f:
    file_config_sql = json.load(f)

conf_values = [ (fields['step_id'], '\n'.join(fields['value'])) for line, value in file_config_sql.items() if line == 'steps' for fields in value ]

# COMMAND ----------

# DBTITLE 1,Update TLSISGRAL_ETL_VAL_MATRIZ_CONV Part1
#UPDATE

query_statement = '002'

params = [sr_folio]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends") 

print(statement)

spark.conf.set("conn_url", str(conn_url))
spark.conf.set("conn_user", str(conn_user))
spark.conf.set("conn_key", str(conn_key))
spark.conf.set("statement", str(statement))

# COMMAND ----------

# DBTITLE 1,Update TLSISGRAL_ETL_VAL_MATRIZ_CONV Part2
# MAGIC %scala
# MAGIC import java.util.Properties
# MAGIC import java.sql.DriverManager
# MAGIC  
# MAGIC val conn_user = spark.conf.get("conn_user")
# MAGIC val conn_key = spark.conf.get("conn_key")
# MAGIC val conn_url = spark.conf.get("conn_url")
# MAGIC val driverClass = "oracle.jdbc.driver.OracleDriver"
# MAGIC
# MAGIC Class.forName(driverClass)
# MAGIC  
# MAGIC val connectionProperties = new Properties()
# MAGIC connectionProperties.setProperty("user", conn_user)
# MAGIC connectionProperties.setProperty("password", conn_key)
# MAGIC connectionProperties.setProperty("v$session.osuser", conn_user)
# MAGIC  
# MAGIC val connection = DriverManager.getConnection(conn_url, connectionProperties)
# MAGIC val stmt = connection.createStatement()
# MAGIC val sql = spark.conf.get("statement")
# MAGIC  
# MAGIC try {
# MAGIC     stmt.execute(sql)
# MAGIC     spark.conf.set("failed_task", "1")
# MAGIC }
# MAGIC catch {
# MAGIC     case _: Throwable => spark.conf.set("failed_task", "0")
# MAGIC } finally {
# MAGIC     connection.close()
# MAGIC }

# COMMAND ----------

# DBTITLE 1,Update TLSISGRAL_ETL_VAL_MATRIZ_CONV Part3
failed_task = spark.conf.get("failed_task")

if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")


# COMMAND ----------

# DBTITLE 1,JP_PANCIN_MCV_0150_MONTOS_CTE_REPR
query_statement = '001'

params = [sr_folio, sr_folio_rel, sr_tipo_mov ,sr_proceso, sr_subproceso]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

df, failed_task = query_table(conn_name_ora, spark, statement,  conn_options, conn_user, conn_key)
df.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog_name}.{schema_name}.{'TEMP_DOIMSS_MCV_MONTOS_CLIENTE' + '_' + sr_folio}"
)
#df.createOrReplaceGlobalTempView('TEMP_DOIMSS_MCV_MONTOS_CLIENTE' + '_' + sr_folio)
#spark.sql("select * from TEMP_DOIMSS_MCV_MONTOS_CLIENTE").show(10)
if debug:
    display(df) 


# COMMAND ----------

if sr_tipo_mov.lower() == 'cargo':
    mode = 'APPEND'
    table_name_001 = 'CIERREN_DATAUX.TTSISGRAL_ETL_MONTOS_CLIENTE_AUX'
 
    failed_task = write_into_table(conn_name_ora, df, mode, table_name_001, conn_options, conn_aditional_options, conn_user, conn_key)
    
    if failed_task == '0':
        logger.error("Please review log messages")
        notification_raised(webhook_url, -1, message, source, input_parameters)
        raise Exception("An error raised")
