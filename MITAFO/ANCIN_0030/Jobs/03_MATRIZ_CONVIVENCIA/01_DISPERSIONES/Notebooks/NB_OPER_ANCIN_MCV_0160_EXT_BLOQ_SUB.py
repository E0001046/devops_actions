# Databricks notebook source
# MAGIC %load_ext autoreload
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
# MAGIC from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
# MAGIC from pyspark.sql.functions import length, lit
# MAGIC
# MAGIC
# MAGIC def input_values():
# MAGIC     """ Retrieve input values from widgets.
# MAGIC     Returns:
# MAGIC         tuple: A tuple containing the input values and a status flag.
# MAGIC     """
# MAGIC     try: 
# MAGIC         #Define widgets de texto en Databricks para capturar valores de entrada.
# MAGIC         dbutils.widgets.text('sr_tipo_mov', '')
# MAGIC         dbutils.widgets.text('sr_proceso', '')
# MAGIC         dbutils.widgets.text('sr_subproceso', '')
# MAGIC         dbutils.widgets.text('sr_origen_arc', '')
# MAGIC         dbutils.widgets.text('sr_dt_org_arc', '')
# MAGIC         dbutils.widgets.text('sr_fecha_acc', '')       
# MAGIC         dbutils.widgets.text('sr_subetapa', '')
# MAGIC         dbutils.widgets.text('sr_folio', '')
# MAGIC         dbutils.widgets.text('sr_folio_rel', '')
# MAGIC         dbutils.widgets.text('sr_reproceso', '')
# MAGIC         dbutils.widgets.text('sr_aeim', '')
# MAGIC
# MAGIC #Recupera los valores de los widgets definidos anteriormente.
# MAGIC         sr_tipo_mov = dbutils.widgets.get('sr_tipo_mov')
# MAGIC         sr_proceso = dbutils.widgets.get('sr_proceso')
# MAGIC         sr_subproceso = dbutils.widgets.get('sr_subproceso')
# MAGIC         sr_origen_arc = dbutils.widgets.get('sr_origen_arc')
# MAGIC         sr_dt_org_arc = dbutils.widgets.get('sr_dt_org_arc')
# MAGIC         sr_fecha_acc = dbutils.widgets.get('sr_fecha_acc')        
# MAGIC         sr_subetapa = dbutils.widgets.get('sr_subetapa')
# MAGIC         sr_folio = dbutils.widgets.get('sr_folio')
# MAGIC         sr_folio_rel = dbutils.widgets.get('sr_folio_rel')
# MAGIC         sr_reproceso = dbutils.widgets.get('sr_reproceso')
# MAGIC         sr_aeim = dbutils.widgets.get('sr_aeim')
# MAGIC
# MAGIC #Verifica si alguno de los valores recuperados está vacío o es nulo
# MAGIC #Si algún valor está vacío, registra un error y retorna una tupla de ceros
# MAGIC         if any(len(str(value).strip()) == 0 for value in [sr_tipo_mov, sr_proceso, sr_subproceso, sr_origen_arc, sr_dt_org_arc, sr_fecha_acc, sr_subetapa, sr_folio, sr_folio_rel, sr_reproceso, sr_aeim]):    
# MAGIC             logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC             logger.error("Some of the input values are empty or null")
# MAGIC             return '0','0','0','0','0','0','0','0','0','0'
# MAGIC     #Captura cualquier excepción que ocurra durante la ejecución del bloque try
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: %s", str(e))
# MAGIC         return '0','0','0','0','0','0','0','0','0','0'
# MAGIC     #Si no hay errores, retorna los valores recuperados y un indicador de éxito ('1')
# MAGIC     return sr_tipo_mov, sr_proceso, sr_subproceso, sr_origen_arc, sr_dt_org_arc, sr_fecha_acc, sr_subetapa, sr_folio, sr_folio_rel, sr_reproceso, sr_aeim, '1'
# MAGIC
# MAGIC
# MAGIC
# MAGIC def conf_process_values(config_file, process_name):
# MAGIC     """ Retrieve process configuration values from a config file.
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
# MAGIC         #Configuración de subprocesos
# MAGIC         #Recupera los valores de configuración específicos para el proceso indicado por process_name
# MAGIC         sql_conf_file = config.get(process_name, 'sql_conf_file_bloqueo_sub')
# MAGIC         conn_schema_001 = config.get(process_name, 'conn_schema_001')
# MAGIC         table_001 = config.get(process_name, 'table_001')
# MAGIC         table_002 = config.get(process_name, 'table_002')
# MAGIC         conn_schema_002 = config.get(process_name, 'conn_schema_002')
# MAGIC         table_003 = config.get(process_name, 'table_003')
# MAGIC         # Unity
# MAGIC         debug = config.get(process_name, "debug")
# MAGIC         debug = debug.lower() == "true"
# MAGIC         catalog_name = config.get(process_name, "catalog_name")
# MAGIC         schema_name = config.get(process_name, "schema_name")
# MAGIC     except Exception as e:
# MAGIC         # Captura cualquier excepción que ocurra durante la ejecución del bloque try.
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC         return '0','0','0','0','0','0','0','0','0','0'
# MAGIC     # Return the retrieved configuration values and a success flag
# MAGIC     return sql_conf_file, conn_schema_001, table_001, table_002, conn_schema_002, table_003, debug, catalog_name, schema_name, '1'
# MAGIC
# MAGIC
# MAGIC #El código principal se ejecuta cuando el script se ejecuta directamente (no cuando se importa como un módulo).
# MAGIC if __name__ == "__main__" :
# MAGIC     #Configura el logger para registrar mensajes de depuración y errores.
# MAGIC     logging.basicConfig()
# MAGIC     logger = logging.getLogger(__name__)
# MAGIC     logger.setLevel(logging.DEBUG)
# MAGIC     #Obtiene el nombre del notebook y define variables para mensajes de error y la ruta del repositorio raíz.
# MAGIC     notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
# MAGIC     message = 'NB Error: ' + notebook_name
# MAGIC     source = 'ETL'
# MAGIC     root_repo = '/Workspace/Shared/MITAFO'
# MAGIC     
# MAGIC     #Intenta importar funciones adicionales desde otros notebooks. Si ocurre un error, lo registra.
# MAGIC     try:
# MAGIC         sys.path.append(root_repo + '/' + 'CGRLS_0010/Notebooks')
# MAGIC         from NB_GRLS_DML_FUNCTIONS import *
# MAGIC         from NB_GRLS_SIMPLE_FUNCTIONS import *
# MAGIC     except Exception as e:
# MAGIC         logger.error("Error at the beggining of the process")
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC
# MAGIC     #Define las rutas de los archivos de configuración necesarios para el proceso.
# MAGIC     config_file = root_repo + '/' + 'CGRLS_0010/Conf/CF_GRLS_PROCESS.py.properties'
# MAGIC     config_conn_file = root_repo + '/' + 'CGRLS_0010/Conf/CF_GRLS_CONN.py.properties'
# MAGIC     config_process_file = root_repo + '/' + 'ANCIN_0030/Jobs/03_MATRIZ_CONVIVENCIA/01_DISPERSIONES/Conf/CF_PART_PROC.py.properties'
# MAGIC
# MAGIC     #Llama a la función input_values para recuperar los valores de entrada. Si hay un error, registra el mensaje y lanza una excepción.
# MAGIC     sr_tipo_mov, sr_proceso, sr_subproceso, sr_origen_arc, sr_dt_org_arc, sr_fecha_acc, sr_subetapa, sr_folio, sr_folio_rel, sr_reproceso, sr_aeim, failed_task = input_values()
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         #notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("An error ocurred, check out log messages")
# MAGIC     
# MAGIC     #Obtiene todos los parámetros de entrada desde los widgets de Databricks.
# MAGIC     input_parameters = dbutils.widgets.getAll().items()
# MAGIC     
# MAGIC     #Llama a la función conf_init_values para recuperar los valores de configuración inicial. Si hay un error, registra el mensaje, envía una notificación y lanza una excepción.
# MAGIC     process_name = 'root'
# MAGIC     webhook_url, channel, failed_task = conf_init_values(config_file, process_name,'MCV')
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     #Llama a la función conf_process_values para recuperar los valores de configuración del proceso. Si hay un error, registra el mensaje, envía una notificación y lanza una excepción.
# MAGIC     process_name = 'root'
# MAGIC     sql_conf_file, conn_schema_001, table_001, table_002, conn_schema_002, table_003, debug, catalog_name, schema_name, failed_task = conf_process_values(config_process_file, process_name)
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     #Llama a la función conf_conn_values para recuperar los valores de configuración de conexión. Si hay un error, registra el mensaje, envía una notificación y lanza una excepción.
# MAGIC     conn_name_ora = 'jdbc_oracle'
# MAGIC     conn_options, conn_aditional_options, conn_user, conn_key, conn_url, scope, failed_task = conf_conn_values(config_conn_file, conn_name_ora)
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC     
# MAGIC     #Definición de la ruta del archivo de configuración SQL
# MAGIC     sql_conf_file = root_repo + '/' + 'ANCIN_0030/Jobs/03_MATRIZ_CONVIVENCIA/01_DISPERSIONES/JSON/' + sql_conf_file
# MAGIC
# MAGIC

# COMMAND ----------

with open(sql_conf_file) as f:
    file_config_sql = json.load(f)

conf_values = [ (fields['step_id'], '\n'.join(fields['value'])) for line, value in file_config_sql.items() if line == 'steps' for fields in value ]

# COMMAND ----------

# DBTITLE 1,JP_PANCIN_MCV_0160_MONTOS_CTE_MV
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

if debug:
    print(statement)
    df.printSchema()
    display(df)

# df.createOrReplaceGlobalTempView('TEMP_DOIMSS_MCV_MONTOS_CLIENTE' + '_' + sr_folio)
#spark.sql("select * from TEMP_DOIMSS_MCV_MONTOS_CLIENTE").show(10)
#display(df) 

