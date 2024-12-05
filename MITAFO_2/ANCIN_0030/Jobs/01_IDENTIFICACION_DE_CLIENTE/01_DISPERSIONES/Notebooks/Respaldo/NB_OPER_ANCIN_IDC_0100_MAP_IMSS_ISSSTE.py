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
# MAGIC def input_values():
# MAGIC     """ Retrieve input values from widgets.
# MAGIC     Returns:
# MAGIC         tuple: A tuple containing the input values and a status flag.
# MAGIC     """
# MAGIC     try:
# MAGIC         dbutils.widgets.text('sr_proceso', '')
# MAGIC         dbutils.widgets.text('sr_subproceso', '')
# MAGIC         dbutils.widgets.text('sr_subetapa', '')
# MAGIC         dbutils.widgets.text('sr_origen_arc', '')
# MAGIC         dbutils.widgets.text('sr_dt_org_arc', '')
# MAGIC         dbutils.widgets.text('sr_folio', '')
# MAGIC         dbutils.widgets.text('sr_id_archivo', '')
# MAGIC         dbutils.widgets.text('sr_tipo_layout', '')
# MAGIC         dbutils.widgets.text('sr_instancia_proceso', '')
# MAGIC         dbutils.widgets.text('sr_usuario', '')
# MAGIC         dbutils.widgets.text('sr_etapa', '')
# MAGIC         dbutils.widgets.text('sr_id_snapshot', '')
# MAGIC         dbutils.widgets.text('sr_paso', '')
# MAGIC
# MAGIC         sr_proceso = dbutils.widgets.get('sr_proceso')
# MAGIC         sr_subproceso = dbutils.widgets.get('sr_subproceso')
# MAGIC         sr_subetapa = dbutils.widgets.get('sr_subetapa')
# MAGIC         sr_origen_arc = dbutils.widgets.get('sr_origen_arc')
# MAGIC         sr_dt_org_arc = dbutils.widgets.get('sr_dt_org_arc')
# MAGIC         sr_folio = dbutils.widgets.get('sr_folio')
# MAGIC         sr_id_archivo = dbutils.widgets.get('sr_id_archivo')
# MAGIC         sr_tipo_layout = dbutils.widgets.get('sr_tipo_layout')
# MAGIC         sr_instancia_proceso = dbutils.widgets.get('sr_instancia_proceso')
# MAGIC         sr_usuario = dbutils.widgets.get('sr_usuario')
# MAGIC         sr_etapa = dbutils.widgets.get('sr_etapa')
# MAGIC         sr_id_snapshot = dbutils.widgets.get('sr_id_snapshot')
# MAGIC         sr_paso = dbutils.widgets.get('sr_paso')
# MAGIC
# MAGIC         if any(len(str(value).strip()) == 0 for value in [sr_proceso, sr_subproceso, sr_subetapa, sr_origen_arc, sr_dt_org_arc, sr_folio, sr_id_archivo, sr_tipo_layout, sr_instancia_proceso, sr_usuario, sr_etapa, sr_id_snapshot, sr_paso]):
# MAGIC             logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC             logger.error("Some of the input values are empty or null")
# MAGIC             return '0','0','0','0','0','0','0','0','0','0','0','0','0','0'
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: %s", str(e))
# MAGIC         return '0','0','0','0','0','0','0','0','0','0','0','0','0','0'
# MAGIC     return sr_proceso, sr_subproceso, sr_subetapa, sr_origen_arc, sr_dt_org_arc, sr_folio, sr_id_archivo, sr_tipo_layout, sr_instancia_proceso, sr_usuario, sr_etapa, sr_id_snapshot, sr_paso, '1'
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
# MAGIC         config = configparser.ConfigParser()
# MAGIC         config.read(config_file)
# MAGIC         #Subprocess configurations
# MAGIC         #
# MAGIC         sql_conf_file = config.get(process_name, 'sql_conf_file')
# MAGIC         conn_schema_001 = config.get(process_name, 'conn_schema_001')
# MAGIC         table_005 = config.get(process_name, 'table_005')
# MAGIC         global_temp_view_004 = config.get(process_name, 'global_temp_view_004')
# MAGIC         global_temp_view_003 = config.get(process_name, 'global_temp_view_003')
# MAGIC         global_temp_view_schema_001 = config.get(process_name, 'global_temp_view_schema_001')
# MAGIC         global_temp_view_005 = config.get(process_name, 'global_temp_view_005')
# MAGIC         var_tramite = config.get(process_name, 'var_tramite')
# MAGIC
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC         return '0','0','0','0','0','0','0','0','0'
# MAGIC     return sql_conf_file, conn_schema_001, table_005, global_temp_view_004,global_temp_view_003,global_temp_view_schema_001,global_temp_view_005,var_tramite, '1'
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
# MAGIC     config_process_file = root_repo + '/' + 'ANCIN_0030/Jobs/01_IDENTIFICACION_DE_CLIENTE/01_DISPERSIONES/Conf/CF_PART_PROC.py.properties'
# MAGIC
# MAGIC     sr_proceso, sr_subproceso, sr_subetapa, sr_origen_arc, sr_dt_org_arc, sr_folio, sr_id_archivo, sr_tipo_layout, sr_instancia_proceso, sr_usuario, sr_etapa, sr_id_snapshot, sr_paso, failed_task = input_values()
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         #notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("An error ocurred, check out log messages")
# MAGIC     
# MAGIC     input_parameters = dbutils.widgets.getAll().items()
# MAGIC     
# MAGIC     process_name = 'root'
# MAGIC     webhook_url, channel, failed_task = conf_init_values(config_file, process_name,'IDC')
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     process_name = 'root'
# MAGIC     sql_conf_file, conn_schema_001, table_005, global_temp_view_004,global_temp_view_003,global_temp_view_schema_001,global_temp_view_005,var_tramite, failed_task = conf_process_values(config_process_file, process_name)
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     conn_name_ora = 'jdbc_oracle'
# MAGIC     conn_options, conn_aditional_options, conn_user, conn_key, conn_url, failed_task = conf_conn_values(config_conn_file, conn_name_ora)
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC     
# MAGIC     sql_conf_file = root_repo + '/' + '/ANCIN_0030/Jobs/01_IDENTIFICACION_DE_CLIENTE/01_DISPERSIONES/JSON/' + sql_conf_file
# MAGIC

# COMMAND ----------

with open(sql_conf_file) as f:
    file_config_sql = json.load(f)

conf_values = [ (fields['step_id'], '\n'.join(fields['value'])) for line, value in file_config_sql.items() if line == 'steps' for fields in value ]

# COMMAND ----------

#conn_schema_001 : CIERREN_ETL
#table_005 : TTSISGRAL_ETL_VAL_IDENT_CTE

table_name_001 = conn_schema_001 + '.' + table_005

query_statement = '004'

params = [table_name_001, sr_id_archivo]

#Asigna a statement el dataframe resultante de la ejecucion de la función getting_statement (obtiene el query a ejecutar)
statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

#print(statement)

#Asigna a df el dataframe resultante de la ejecucion de la función query_table (realiza consulta en oracle)
df, failed_task = query_table(conn_name_ora, spark, statement,  conn_options, conn_user, conn_key)

if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")

#df.count()

# COMMAND ----------

var_tramite

# COMMAND ----------

# DBTITLE 1,CREACIÓN VISTA GLOBAL TEMP_MAP_IMSS_ISSSTE
temp_view = global_temp_view_005 + '_' + sr_id_archivo
#print(temp_view)
#spark.catalog.dropGlobalTempView(temp_view)

df.createOrReplaceGlobalTempView(temp_view)


# COMMAND ----------

print(var_tramite)
if var_tramite == 'DOIMSS':
    var_tramite = 'IMSS' 
else: var_tramite = 'ISSSTE'  

print(var_tramite)

query_statement = '005'

params = [var_tramite,sr_id_archivo,conn_user]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

print(statement)

#Asigna a df el dataframe resultante de la ejecucion de la función  spark.sql(statement) (realiza consulta en Databricks)
df = spark.sql(statement)


#display(df)


# COMMAND ----------

# DBTITLE 1,CREACION VISTA GLOBAL TEMP_DISP_IMSS_ISSSTE
temp_view = 'TEMP_DISP_IMSS_ISSSTE' + '_' + sr_id_archivo

df1 = df.select('FTC_FOLIO','FTN_ID_ARCHIVO','FTN_NSS','FTC_CURP','FTC_RFC','FTC_NOMBRE_CTE','FTC_CLAVE_ENT_RECEP','FTC_CLAVE_TMP')
df1.createOrReplaceGlobalTempView(temp_view)



# COMMAND ----------

df2 = df.select('FTC_FOLIO','FTC_NSS_CURP','FTD_FEH_CRE','FTC_USU_CRE')
#display(df2)


# COMMAND ----------

# DBTITLE 1,DELETE ORACLE DISPERSION_CTAS
#DELETE tabla CIERREN_ETL.TTSISGRAL_ETL_DISPERSION_CTAS

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

# DBTITLE 1,ASIGNA VARIABLES DELETE
spark.conf.set("conn_url", str(conn_url))
spark.conf.set("conn_user", str(conn_user))
spark.conf.set("conn_key", str(conn_key))
spark.conf.set("statement", str(statement))

# COMMAND ----------

# DBTITLE 1,EJECUTA DELETE
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

# DBTITLE 1,REVISIÓN EXCEPCIONES
failed_task = spark.conf.get("failed_task")

if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")


# COMMAND ----------

#display(df2)

# COMMAND ----------

# DBTITLE 1,INSERCION ORACLE DISPERSION_CTAS
#INSERCIÓN tabla CIERREN_ETL.TTSISGRAL_ETL_DISPERSION_CTAS
mode = 'APPEND'

table_name_001 = 'CIERREN_ETL.TTSISGRAL_ETL_DISPERSION_CTAS'
failed_task = write_into_table(conn_name_ora, df2, mode, table_name_001, conn_options, conn_aditional_options, conn_user, conn_key)

if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")