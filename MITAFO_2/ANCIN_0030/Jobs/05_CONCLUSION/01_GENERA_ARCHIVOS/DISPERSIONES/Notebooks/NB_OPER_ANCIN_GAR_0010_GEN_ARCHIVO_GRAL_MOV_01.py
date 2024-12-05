# Databricks notebook source
# DBTITLE 1,CONFIGURACIONES PRINCIPALES
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC %reload_ext autoreload
# MAGIC
# MAGIC import sys
# MAGIC import inspect
# MAGIC import configparser
# MAGIC import json
# MAGIC import logging
# MAGIC
# MAGIC from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
# MAGIC from pyspark.sql.functions import length, lit
# MAGIC
# MAGIC def input_values():
# MAGIC     """ Recuperar valores de entrada desde widgets.
# MAGIC     Returns:
# MAGIC         tuple: Una tupla que contiene los valores de entrada y una bandera de estado.
# MAGIC     """
# MAGIC     try:
# MAGIC         dbutils.widgets.text('sr_folio', '')
# MAGIC         dbutils.widgets.text('sr_proceso', '')
# MAGIC         dbutils.widgets.text('sr_subproceso', '')
# MAGIC         dbutils.widgets.text('sr_origen_arc', '')
# MAGIC         dbutils.widgets.text('sr_dt_org_arc', '')
# MAGIC         dbutils.widgets.text('sr_subetapa', '')
# MAGIC         dbutils.widgets.text('sr_sec_lote', '')
# MAGIC         dbutils.widgets.text('sr_fecha_lote', '')
# MAGIC         dbutils.widgets.text('sr_fecha_acc', '')
# MAGIC         dbutils.widgets.text('sr_tipo_archivo', '')
# MAGIC         dbutils.widgets.text('sr_estatus_mov', '')
# MAGIC         dbutils.widgets.text('sr_tipo_mov', '')
# MAGIC         dbutils.widgets.text('sr_accion', '')
# MAGIC        
# MAGIC         # Obtener valores de los widgets
# MAGIC         sr_folio = dbutils.widgets.get('sr_folio')
# MAGIC         sr_proceso = dbutils.widgets.get('sr_proceso')
# MAGIC         sr_subproceso = dbutils.widgets.get('sr_subproceso')
# MAGIC         sr_origen_arc = dbutils.widgets.get('sr_origen_arc')
# MAGIC         sr_dt_org_arc = dbutils.widgets.get('sr_dt_org_arc')
# MAGIC         sr_subetapa = dbutils.widgets.get('sr_subetapa')
# MAGIC         sr_sec_lote = dbutils.widgets.get('sr_sec_lote')
# MAGIC         sr_fecha_lote = dbutils.widgets.get('sr_fecha_lote')
# MAGIC         sr_fecha_acc = dbutils.widgets.get('sr_fecha_acc')
# MAGIC         sr_tipo_archivo = dbutils.widgets.get('sr_tipo_archivo')
# MAGIC         sr_estatus_mov = dbutils.widgets.get('sr_estatus_mov')
# MAGIC         sr_tipo_mov = dbutils.widgets.get('sr_tipo_mov')
# MAGIC         sr_accion = dbutils.widgets.get('sr_accion')
# MAGIC
# MAGIC         # Verificar si algún valor de entrada está vacío o nulo
# MAGIC         if any(len(str(value).strip()) == 0 for value in [sr_folio,sr_proceso,sr_subproceso,sr_subetapa,sr_sec_lote,sr_fecha_lote,sr_tipo_archivo]):    
# MAGIC             logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC             logger.error("Some of the input values are empty or null")
# MAGIC             return '0','0','0','0','0','0','0'
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: %s", str(e))
# MAGIC         return '0','0','0','0','0','0','0','0','0','0','0','0','0','0'
# MAGIC     return sr_folio,sr_proceso,sr_subproceso,sr_origen_arc,sr_dt_org_arc,sr_subetapa,sr_sec_lote,sr_fecha_lote,sr_fecha_acc,sr_tipo_archivo,sr_estatus_mov,sr_tipo_mov,sr_accion,'1'
# MAGIC
# MAGIC def conf_process_values(config_file, process_name):
# MAGIC     """ Recuperar valores de configuración del proceso desde un archivo de configuración.
# MAGIC     Args:
# MAGIC         config_file (str): Ruta al archivo de configuración.
# MAGIC         process_name (str): Nombre del proceso.
# MAGIC     Returns:
# MAGIC         tuple: Una tupla que contiene los valores de configuración y una bandera de estado.
# MAGIC     """
# MAGIC     try:
# MAGIC         config = configparser.ConfigParser()
# MAGIC         config.read(config_file)
# MAGIC         # Configuraciones del subproceso
# MAGIC         sql_conf_file = config.get(process_name, 'sql_conf_file')  # Obtener archivo de configuración SQL
# MAGIC         conn_schema_001 = config.get(process_name, 'conn_schema_001')
# MAGIC         conn_schema_002 = config.get(process_name,'conn_schema_002')
# MAGIC         table_001 = config.get(process_name, 'table_001')
# MAGIC         table_003 = config.get(process_name, 'table_003')
# MAGIC         table_004 = config.get(process_name, 'table_004')
# MAGIC         table_005 = config.get(process_name, 'table_005')
# MAGIC         table_006 = config.get(process_name, 'table_006')
# MAGIC         table_007 = config.get(process_name, 'table_007')
# MAGIC         table_008 = config.get(process_name, 'table_008')
# MAGIC         var_001 = config.get(process_name, 'var_001')
# MAGIC         debug = config.get(process_name, "debug")  # Obtener valor de debug
# MAGIC         debug = debug.lower() == "true"  # Convertir valor de debug a booleano
# MAGIC         catalog_name = config.get(process_name, "catalog_name")
# MAGIC         schema_name = config.get(process_name, "schema_name")
# MAGIC     
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC         return '0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0'
# MAGIC     return sql_conf_file, conn_schema_001, conn_schema_002,table_001, table_003,table_004,table_005,table_006,table_007,table_008,var_001,debug,catalog_name,schema_name,'1'
# MAGIC
# MAGIC if __name__ == "__main__" :
# MAGIC     logging.basicConfig()  # Configurar logging básico
# MAGIC     logger = logging.getLogger(__name__)  # Obtener logger
# MAGIC     logger.setLevel(logging.DEBUG)  # Establecer nivel de logging a DEBUG
# MAGIC     notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()  # Obtener nombre del notebook
# MAGIC     message = 'NB Error: ' + notebook_name  # Mensaje de error
# MAGIC     source = 'ETL'  # Fuente del mensaje
# MAGIC     root_repo = '/Workspace/Shared/MITAFO'  # Ruta del repositorio raíz
# MAGIC     
# MAGIC     try:
# MAGIC         sys.path.append(root_repo + '/' + 'CGRLS_0010/Notebooks')  # Agregar ruta del notebook al sys.path
# MAGIC         from NB_GRLS_DML_FUNCTIONS import *  # Importar funciones DML
# MAGIC         from NB_GRLS_SIMPLE_FUNCTIONS import *  # Importar funciones simples
# MAGIC     except Exception as e:
# MAGIC         logger.error("Error at the beggining of the process")
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC
# MAGIC     config_file = root_repo + '/' + 'CGRLS_0010/Conf/CF_GRLS_PROCESS.py.properties'  # Ruta del archivo de configuración del proceso
# MAGIC     config_conn_file = root_repo + '/' + 'CGRLS_0010/Conf/CF_GRLS_CONN.py.properties'  # Ruta del archivo de configuración de conexión
# MAGIC     config_process_file = root_repo + '/' + 'ANCIN_0030/Jobs/05_CONCLUSION/01_GENERA_ARCHIVOS/DISPERSIONES/Conf/CF_PART_PROC.py.properties'  # Ruta del archivo de configuración del proceso específico
# MAGIC
# MAGIC     sr_folio,sr_proceso,sr_subproceso,sr_origen_arc,sr_dt_org_arc,sr_subetapa,sr_sec_lote,sr_fecha_lote,sr_fecha_acc,sr_tipo_archivo,sr_estatus_mov,sr_tipo_mov,sr_accion,failed_task= input_values()  # Obtener valores de entrada
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         #notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("An error ocurred, check out log messages")
# MAGIC     
# MAGIC     input_parameters = dbutils.widgets.getAll().items()  # Obtener todos los parámetros de entrada
# MAGIC     
# MAGIC     process_name = 'root'
# MAGIC     webhook_url, channel, failed_task = conf_init_values(config_file, process_name,'CTINDI')  # Obtener valores de configuración inicial
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     process_name = 'root'
# MAGIC     sql_conf_file, conn_schema_001, conn_schema_002, table_001, table_003, table_004, table_005, table_006, table_007, table_008, var_001, debug, catalog_name,schema_name, failed_task = conf_process_values(config_process_file, process_name)
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     conn_name_ora = 'jdbc_oracle'
# MAGIC     conn_options, conn_aditional_options, conn_user, conn_key, conn_url, scope, failed_task = conf_conn_values(config_conn_file, conn_name_ora)  # Obtener valores de configuración de conexión
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC     
# MAGIC     sql_conf_file = root_repo + '/' + 'ANCIN_0030/Jobs/05_CONCLUSION/01_GENERA_ARCHIVOS/DISPERSIONES/JSON/' + sql_conf_file  # Actualizar ruta del archivo de configuración SQL

# COMMAND ----------

# DBTITLE 1,EXTRACCIÓN DE ARCHIVO JSON
with open(sql_conf_file) as f:
    file_config_sql = json.load(f)

conf_values = [ (fields['step_id'], '\n'.join(fields['value'])) for line, value in file_config_sql.items() if line == 'steps' for fields in value ]

# COMMAND ----------

# DBTITLE 1,CONSTRUCCIÓN DE CONSULTA
# Extracción de información correspondiente a MOVIMIENTOS
table_name_001 = conn_schema_001 + '.' + table_001
table_name_002 = conn_schema_002 + '.' + table_003
table_name_003 = conn_schema_002 + '.' + table_004
table_name_004 = conn_schema_002 + '.' + table_005
table_name_005 = conn_schema_002 + '.' + table_006
table_name_006 = conn_schema_002 + '.' + table_007
table_name_007 = conn_schema_002 + '.' + table_008
var_001 = var_001

query_statement = '002'

params = [sr_fecha_lote,sr_sec_lote,table_name_001,table_name_002,table_name_003,table_name_004,table_name_005,table_name_006,table_name_007,sr_folio,var_001]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

if debug:
    print(statement)

# COMMAND ----------

# DBTITLE 1,EXTRACCIÓN DE INFORMACIÓN
df, failed_task = query_table(conn_name_ora, spark, statement,  conn_options, conn_user, conn_key)

# COMMAND ----------

# DBTITLE 1,CREACIÓN DE  TABLA DELTA
#df.createOrReplaceGlobalTempView(temp_view)
temp_view = f"TEMP_DOIMSS_CTI_01_MOVIMIENTOS_{sr_folio}"
df.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog_name}.{schema_name}.{temp_view}"
)

if debug:
    display(spark.sql(f"SELECT * FROM {catalog_name}.{schema_name}.{temp_view}"))