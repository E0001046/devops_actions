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
# MAGIC
# MAGIC from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
# MAGIC from pyspark.sql.functions import length, lit
# MAGIC
# MAGIC def input_values():
# MAGIC     """ Retrieve input values from widgets.
# MAGIC     Returns:
# MAGIC         tuple: A tuple containing the input values and a status flag.
# MAGIC     """
# MAGIC     try:
# MAGIC         dbutils.widgets.text('sr_tipo_reporte', '')
# MAGIC         dbutils.widgets.text('sr_proceso', '')
# MAGIC         dbutils.widgets.text('sr_subproceso', '')
# MAGIC         dbutils.widgets.text('sr_origen_arc', '')
# MAGIC         dbutils.widgets.text('sr_dt_org_arc', '')
# MAGIC         dbutils.widgets.text('sr_subetapa', '')
# MAGIC         dbutils.widgets.text('sr_folio', '')
# MAGIC         dbutils.widgets.text('sr_id_archivo', '')
# MAGIC         dbutils.widgets.text('sr_fec_arc', '')
# MAGIC         dbutils.widgets.text('sr_fec_liq', '')
# MAGIC         dbutils.widgets.text('sr_usuario','')
# MAGIC         dbutils.widgets.text('sr_etapa','')
# MAGIC         dbutils.widgets.text('sr_id_snapshot','')
# MAGIC         dbutils.widgets.text('sr_instancia_proceso','')
# MAGIC
# MAGIC
# MAGIC         sr_tipo_reporte = dbutils.widgets.get('sr_tipo_reporte')
# MAGIC         sr_proceso = dbutils.widgets.get('sr_proceso')
# MAGIC         sr_subproceso = dbutils.widgets.get('sr_subproceso')
# MAGIC         sr_origen_arc = dbutils.widgets.get('sr_origen_arc')
# MAGIC         sr_dt_org_arc = dbutils.widgets.get('sr_dt_org_arc')
# MAGIC         sr_subetapa = dbutils.widgets.get('sr_subetapa')
# MAGIC         sr_folio = dbutils.widgets.get('sr_folio')
# MAGIC         sr_id_archivo = dbutils.widgets.get('sr_id_archivo')
# MAGIC         sr_fec_arc = dbutils.widgets.get('sr_fec_arc')
# MAGIC         sr_fec_liq = dbutils.widgets.get('sr_fec_liq')
# MAGIC         sr_usuario = dbutils.widgets.get('sr_usuario')
# MAGIC         sr_etapa = dbutils.widgets.get('sr_etapa')
# MAGIC         sr_id_snapshot = dbutils.widgets.get('sr_id_snapshot')
# MAGIC         sr_instancia_proceso = dbutils.widgets.get('sr_instancia_proceso')
# MAGIC
# MAGIC         if any(len(str(value).strip()) == 0 for value in [sr_tipo_reporte, sr_proceso, sr_subproceso, sr_origen_arc, sr_dt_org_arc, sr_subetapa, sr_folio, sr_id_archivo, sr_fec_arc, sr_fec_liq,sr_usuario,sr_etapa,sr_id_snapshot,sr_instancia_proceso]):    
# MAGIC             logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC             logger.error("Some of the input values are empty or null")
# MAGIC             return '0','0','0','0','0','0','0','0','0','0','0','0','0','0'
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: %s", str(e))
# MAGIC         return '0','0','0','0','0','0','0','0','0','0','0','0','0','0'
# MAGIC     return sr_tipo_reporte, sr_proceso, sr_subproceso, sr_origen_arc, sr_dt_org_arc, sr_subetapa, sr_folio, sr_id_archivo, sr_fec_arc, sr_fec_liq,sr_usuario,sr_etapa,sr_id_snapshot,sr_instancia_proceso, '1'
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
# MAGIC         config = configparser.ConfigParser()
# MAGIC         config.read(config_file)
# MAGIC         #Subprocess configurations
# MAGIC         #
# MAGIC         sql_conf_file = config.get(process_name, 'sql_conf_file')
# MAGIC         global_temp_view_schema_001 = config.get(process_name, 'global_temp_view_schema_001')
# MAGIC         global_temp_view_001 = config.get(process_name, 'global_temp_view_001')
# MAGIC         global_temp_view_002 = config.get(process_name, 'global_temp_view_002')
# MAGIC         global_temp_view_003 = config.get(process_name, 'global_temp_view_003')
# MAGIC         temp_view_003 = config.get(process_name, 'temp_view_003')
# MAGIC         conn_schema_001 = config.get(process_name, 'conn_schema_001')
# MAGIC         conn_schema_002 = config.get(process_name, 'conn_schema_002')
# MAGIC         table_001 = config.get(process_name, 'table_001')
# MAGIC         table_002 = config.get(process_name, 'table_002')
# MAGIC         table_003 = config.get(process_name, 'table_023')
# MAGIC         
# MAGIC         
# MAGIC
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC         return '0','0','0','0','0','0','0'
# MAGIC     return sql_conf_file, global_temp_view_schema_001,conn_schema_002,global_temp_view_001, global_temp_view_002,global_temp_view_003, temp_view_003,conn_schema_001,table_001,table_002,table_003, '1'
# MAGIC
# MAGIC
# MAGIC
# MAGIC if __name__ == "__main__" :
# MAGIC     logging.basicConfig()
# MAGIC     logger = logging.getLogger(__name__)
# MAGIC     logger.setLevel(logging.DEBUG)
# MAGIC     notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
# MAGIC     message = 'NB Error: ' + notebook_name
# MAGIC     source = 'ETL'
# MAGIC     root_repo = '/Workspace/Shared/MITAFO'
# MAGIC     
# MAGIC     try:
# MAGIC         sys.path.append(root_repo + '/' + 'CGRLS_0010/Notebooks')
# MAGIC         from NB_GRLS_DML_FUNCTIONS import *
# MAGIC         from NB_GRLS_SIMPLE_FUNCTIONS import *
# MAGIC     except Exception as e:
# MAGIC         logger.error("Error at the beggining of the process")
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC
# MAGIC     config_file = root_repo + '/' + 'CGRLS_0010/Conf/CF_GRLS_PROCESS.py.properties'
# MAGIC     config_conn_file = root_repo + '/' + 'CGRLS_0010/Conf/CF_GRLS_CONN.py.properties'
# MAGIC     config_process_file = root_repo + '/' + 'AGRLS_0010/Jobs/03_CONCILIACION/02_CIFRAS_CONTROL/Conf/CF_PART_PROC.py.properties'
# MAGIC
# MAGIC     
# MAGIC     sr_tipo_reporte, sr_proceso, sr_subproceso, sr_origen_arc, sr_dt_org_arc, sr_subetapa, sr_folio, sr_id_archivo, sr_fec_arc, sr_fec_liq,sr_usuario,sr_etapa,sr_id_snapshot,sr_instancia_proceso,failed_task = input_values()
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("An error ocurred, check out log messages")
# MAGIC     
# MAGIC     input_parameters = dbutils.widgets.getAll().items()
# MAGIC     
# MAGIC     process_name = 'root'
# MAGIC     webhook_url, channel, failed_task = conf_init_values(config_file, process_name,'GCC')
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     process_name = 'root'
# MAGIC     sql_conf_file, global_temp_view_schema_001,conn_schema_002, global_temp_view_001, global_temp_view_002,global_temp_view_003, temp_view_003,conn_schema_001,table_001,table_002,table_003, failed_task = conf_process_values(config_process_file, process_name)
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     conn_name_ora = 'jdbc_oracle'
# MAGIC     conn_options, conn_aditional_options, conn_user, conn_key, conn_url,scope, failed_task = conf_conn_values(config_conn_file, conn_name_ora)
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC     
# MAGIC     sql_conf_file = root_repo + '/' + 'AGRLS_0010/Jobs/03_CONCILIACION/02_CIFRAS_CONTROL/JSON/' + sql_conf_file
# MAGIC

# COMMAND ----------

with open(sql_conf_file) as f:
    file_config_sql = json.load(f)

conf_values = [ (fields['step_id'], '\n'.join(fields['value'])) for line, value in file_config_sql.items() if line == 'steps' for fields in value ]

# COMMAND ----------

# DBTITLE 1,4TA PARTE - UNIÓN DE REGISTROS

temp_view_001 = global_temp_view_schema_001 + '.' + global_temp_view_001 + '_' + sr_id_archivo
temp_view_002 = global_temp_view_schema_001 + '.' + global_temp_view_002 + '_' + sr_id_archivo


query_statement = '003'

params = [temp_view_001,temp_view_002]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

# COMMAND ----------

# DBTITLE 1,CONSTRUCCIÓN DE CONSULTA
df, failed_task = query_table(conn_name_ora, spark, statement,  conn_options, conn_user, conn_key)

# COMMAND ----------

# DBTITLE 1,CREACIÓN DE VISTA TEMPORAL
df = spark.sql(statement)
df.createOrReplaceTempView(temp_view_003)

# COMMAND ----------

# DBTITLE 1,SALIDA FINAL
temp_view_004 = global_temp_view_schema_001 + '.' + global_temp_view_003 + '_' + sr_id_archivo

query_statement = '005'

params = [sr_folio,temp_view_003,sr_subproceso,temp_view_004]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

# COMMAND ----------

# DBTITLE 1,CONSTRUCCIÓN DE CONSULTA
df, failed_task = query_table(conn_name_ora, spark, statement,  conn_options, conn_user, conn_key)

# COMMAND ----------

# DBTITLE 1,ASIGNACIÓN
df = spark.sql(statement)

# COMMAND ----------

# DBTITLE 1,CONSTRUCCIÓN DEL DELETE
# Antes de insertar, asegurar que no exista la información
# DELETE tabla CIERREN.TLAFOGRAL_SUMARIO_ARCHIVO

query_statement = '006'

params = [sr_folio]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

#print(statement)

# COMMAND ----------

# DBTITLE 1,ASIGNACIÓN VARIABLES SCALA
spark.conf.set("conn_url", str(conn_url))
spark.conf.set("conn_user", str(conn_user))
spark.conf.set("conn_key", str(conn_key))
spark.conf.set("statement", str(statement))
spark.conf.set("scope", str(scope))

# COMMAND ----------

# DBTITLE 1,EJECUTA EL BORRADO DE LA INFORMACIÓN
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

# DBTITLE 1,REVISIÓN DE EXCEPCIONES
failed_task = spark.conf.get("failed_task")

if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")

# COMMAND ----------

# DBTITLE 1,INSERCIÓN DE INFORMACIÓN A TABLA SUMARIO_ARCHIVO
mode = 'APPEND'

table_name_003 = conn_schema_002 + '.' + table_003
#CIERREN.TLAFOGRAL_SUMARIO_ARCHIVO
failed_task = write_into_table(conn_name_ora, df, mode, table_name_003, conn_options, conn_aditional_options, conn_user, conn_key)

if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")

# COMMAND ----------

'''spark.catalog.dropGlobalTempView("TEMP_DISPERSION_CIFRAS" + '_' + sr_id_archivo)
spark.catalog.dropGlobalTempView("TEMP_CONCILIACIONES_CIFRAS" + '_' + sr_id_archivo)
spark.catalog.dropGlobalTempView("TEMP_CONTEO_REGCONYSINVIV" + '_' + sr_id_archivo)'''


# COMMAND ----------

#End process notification
notification_raised(webhook_url, 0, "Done", source, input_parameters)
