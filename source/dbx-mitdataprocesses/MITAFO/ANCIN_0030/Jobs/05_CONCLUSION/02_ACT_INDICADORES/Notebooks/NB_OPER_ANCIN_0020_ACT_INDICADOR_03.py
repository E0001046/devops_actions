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
# MAGIC
# MAGIC def input_values():
# MAGIC     """ Retrieve input values from widgets.
# MAGIC     Returns:
# MAGIC         tuple: A tuple containing the input values and a status flag.
# MAGIC     """
# MAGIC     try:
# MAGIC         #Input var values
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
# MAGIC         dbutils.widgets.text('sr_instancia_proceso', '')
# MAGIC         dbutils.widgets.text('sr_usuario', '')
# MAGIC         dbutils.widgets.text('sr_etapa', '')
# MAGIC         dbutils.widgets.text('sr_id_snapshot', '')
# MAGIC
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
# MAGIC         sr_instancia_proceso = dbutils.widgets.get('sr_instancia_proceso')
# MAGIC         sr_usuario = dbutils.widgets.get('sr_usuario')
# MAGIC         sr_etapa = dbutils.widgets.get('sr_etapa')
# MAGIC         sr_id_snapshot = dbutils.widgets.get('sr_id_snapshot')
# MAGIC
# MAGIC         if any(len(str(value).strip()) == 0 for value in [sr_folio,sr_proceso,sr_subproceso,sr_subetapa,sr_instancia_proceso, sr_usuario, sr_etapa, sr_id_snapshot]):    
# MAGIC             logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC             logger.error("Some of the input values are empty or null")
# MAGIC             return '0','0','0','0','0','0','0','0'
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: %s", str(e))
# MAGIC         return '0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0'
# MAGIC     return sr_folio,sr_proceso,sr_subproceso,sr_origen_arc,sr_dt_org_arc,sr_subetapa,sr_sec_lote,sr_fecha_lote,sr_fecha_acc,sr_tipo_archivo,sr_estatus_mov,sr_tipo_mov,sr_accion, sr_instancia_proceso, sr_usuario, sr_etapa, sr_id_snapshot,'1'
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
# MAGIC         #Please review de Conf file and verify all schemas, tables & views
# MAGIC         sql_conf_file = config.get(process_name, 'sql_conf_file')
# MAGIC         conn_schema_001 = config.get(process_name, 'conn_schema_001')
# MAGIC         table_001 = config.get(process_name, 'table_001')
# MAGIC         conn_schema_003 = config.get(process_name, 'conn_schema_003')
# MAGIC         table_003 = config.get(process_name, 'table_003')
# MAGIC         conn_schema_004 = config.get(process_name, 'conn_schema_004')
# MAGIC         debug = config.get(process_name, "debug")  # Obtener valor de debug
# MAGIC         debug = debug.lower() == "true"  # Convertir valor de debug a booleano
# MAGIC         catalog_name = config.get(process_name, "catalog_name")
# MAGIC         schema_name = config.get(process_name, "schema_name")
# MAGIC
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC         return '0','0','0','0','0','0','0','0','0','0'
# MAGIC     return sql_conf_file,conn_schema_001,table_001,conn_schema_003,table_003,conn_schema_004,debug,catalog_name,schema_name,'1'
# MAGIC
# MAGIC
# MAGIC #Main function
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
# MAGIC     #Get the values from the workspace paths
# MAGIC     config_file = root_repo + '/' + 'CGRLS_0010/Conf/CF_GRLS_PROCESS.py.properties'
# MAGIC     config_conn_file = root_repo + '/' + 'CGRLS_0010/Conf/CF_GRLS_CONN.py.properties'
# MAGIC     config_process_file = root_repo + '/' + 'ANCIN_0030/Jobs/05_CONCLUSION/02_ACT_INDICADORES/Conf/CF_PART_PROC.py.properties'
# MAGIC
# MAGIC     sr_folio,sr_proceso,sr_subproceso,sr_origen_arc,sr_dt_org_arc,sr_subetapa,sr_sec_lote,sr_fecha_lote,sr_fecha_acc,sr_tipo_archivo,sr_estatus_mov,sr_tipo_mov,sr_accion, sr_instancia_proceso, sr_usuario, sr_etapa, sr_id_snapshot, failed_task= input_values()
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         #notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("An error ocurred, check out log messages")
# MAGIC     
# MAGIC     input_parameters = dbutils.widgets.getAll().items()
# MAGIC     
# MAGIC     process_name = 'root'
# MAGIC     webhook_url, channel, failed_task = conf_init_values(config_file, process_name, 'ACT_INDI')
# MAGIC
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         #notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     process_name = 'root'
# MAGIC     
# MAGIC     sql_conf_file,conn_schema_001,table_001,conn_schema_003,table_003,conn_schema_004,debug,catalog_name,schema_name,failed_task = conf_process_values(config_process_file, process_name)
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     conn_name_ora = 'jdbc_oracle'
# MAGIC     conn_options, conn_aditional_options, conn_user, conn_key, conn_url, scope, failed_task = conf_conn_values(config_conn_file, conn_name_ora)
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC     
# MAGIC     #Json file path
# MAGIC     sql_conf_file = root_repo + '/' + 'ANCIN_0030/Jobs/05_CONCLUSION/02_ACT_INDICADORES/JSON/' + sql_conf_file
# MAGIC

# COMMAND ----------

# DBTITLE 1,EXTRACCIÓN DE ARCHIVO JSON
#Read Json file & extract the values 
with open(sql_conf_file) as f:
    file_config_sql = json.load(f)

conf_values = [ (fields['step_id'], '\n'.join(fields['value'])) for line, value in file_config_sql.items() if line == 'steps' for fields in value ]

# COMMAND ----------

# DBTITLE 1,CONSTRUCCIÓN DE CONSULTA UNIÓN DE INDICADORES
# Definición de la tabla 004 (TEMP_DOIMSS_ACT_IND_02)
table_004 = 'TEMP_DOIMSS_ACT_IND_02'

table_name_004 =  catalog_name + '.' + schema_name + '.' + table_004 + '_'+ sr_folio


# Declaración de la consulta 003
query_statement = '003'

# Parámetros para la consulta
params = [table_name_004]

# Obtiene la declaración de la consulta
statement, failed_task = getting_statement(conf_values, query_statement, params)

# Verifica si la tarea falló
if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

# Si está en modo debug, imprime la declaración de la consulta
if debug:
    print(statement)

# COMMAND ----------

# DBTITLE 1,UNIÓN DE INDICADORES
# Ejecuta la consulta SQL y obtiene el DataFrame resultante
df_union_indi = spark.sql(statement)


# Si está en modo debug True, muestra el DataFrame resultante
if debug:
    display(df_union_indi)

# COMMAND ----------

# DBTITLE 1,ELIMINACIÓN DE DUPLICADOS
df_final = df_union_indi.dropDuplicates(["FCN_ID_IND_CTA_INDV", "FTN_NUM_CTA_INVDUAL", "FFN_ID_CONFIG_INDI", "FCC_VALOR_IND", "FTC_VIGENCIA"])

# Si está en modo debug True, muestra el DataFrame resultante
if debug:
    display(df_final)

# COMMAND ----------

# DBTITLE 1,FILTRO CUENTA INDIVIDUAL NULA
from pyspark.sql.functions import col, isnull, isnotnull

# Registros con una cuenta individual nula  (FTN_ID_CTA_INVDUAL)
df_nulos = df_final.filter(isnull(col('FCN_ID_IND_CTA_INDV')))

# Si está en modo debug True, muestra el DataFrame resultante
if debug:
    display(df_nulos)


# COMMAND ----------

# DBTITLE 1,INSERTA REGISTROS A TABLA IND_CTA_INV
#CIERREN.TTAFOGRAL_IND_CTA_INDV

if df_nulos.head(1):

    mode = 'APPEND'

    table_name_001 = conn_schema_001 + '.' + table_001

    failed_task = write_into_table(conn_name_ora, df_nulos, mode, table_name_001, conn_options, conn_aditional_options, conn_user, conn_key)

    if failed_task == '0':
        logger.error("Please review log messages")
        notification_raised(webhook_url, -1, message, source, input_parameters)
        raise Exception("An error raised")


# COMMAND ----------

# DBTITLE 1,FILTRO CUENTA INDIVIDUAL NO NULA
from pyspark.sql.functions import col, isnotnull, lit

# Registros con una cuenta individual NO nula  (FTN_ID_CTA_INVDUAL)
df_no_nulos = df_final.filter(isnotnull(col('FCN_ID_IND_CTA_INDV')))

# Agregar una nueva columna 'SR_FOLIO' al DataFrame df_no_nulos
df_no_nulos_folio = df_no_nulos.withColumn('FTC_FOLIO', lit(sr_folio))

# Si está en modo debug True, muestra el DataFrame resultante
if debug:
    display(df_no_nulos_folio)


# COMMAND ----------

# DBTITLE 1,LLENADO DE LA TABLA AUXILIAR PARA UPDATE
#INSERCIÓN tabla CIERREN_DATAUX.TTAFOGRAL_ETL_IND_CTA_INDV_AUX
mode = 'APPEND'

table_name_002 = 'CIERREN_DATAUX.TTAFOGRAL_ETL_IND_CTA_INDV_AUX'
failed_task = write_into_table(conn_name_ora, df_no_nulos_folio, mode, table_name_002, conn_options, conn_aditional_options, conn_user, conn_key)


if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")


# COMMAND ----------

# DBTITLE 1,INVOCACIÓN DE MERGE PARA UPDATE
# Declaración de la consulta 003
query_statement = '004'

table_name_001 = 'CIERREN.TTAFOGRAL_IND_CTA_INDV'
table_name_002 = 'CIERREN_DATAUX.TTAFOGRAL_ETL_IND_CTA_INDV_AUX'

# Parámetros para la consulta
params = [table_name_001, table_name_002, sr_folio]

# Obtiene la declaración de la consulta
statement, failed_task = getting_statement(conf_values, query_statement, params)

# Verifica si la tarea falló
if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

# Si está en modo debug, imprime la declaración de la consulta
if debug:
    print(statement)

# COMMAND ----------

# DBTITLE 1,SE CARGAN LAS VARIABLES
spark.conf.set("conn_url", str(conn_url))
spark.conf.set("conn_user", str(conn_user))
spark.conf.set("conn_key", str(conn_key))
spark.conf.set("statement", str(statement))
spark.conf.set("scope", str(scope))

# COMMAND ----------

# DBTITLE 1,SE ENVIA EL MERGE CON SCALA
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

# DBTITLE 1,SE DEPURA LA TABLA AUXILIAR
statement = f"""
DELETE FROM CIERREN_DATAUX.TTAFOGRAL_ETL_IND_CTA_INDV_AUX
WHERE FTC_FOLIO = '{sr_folio}'
"""
statement

# COMMAND ----------

# Esta celda establece la configuración de Spark para la variable 'statement' con el valor de la cadena SQL proporcionada.
spark.conf.set("statement", str(statement))

# COMMAND ----------

# DBTITLE 1,CIERRE DE CONEXIONES
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

#Manejo de errores de la operación realizada en Scala
failed_task = spark.conf.get("failed_task")

if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")


# COMMAND ----------

# DBTITLE 1,Notificación finalizado
# Habilitar esta linea cuando halla webhook para GENERACION DE MOVIENTOS Y REEMPLAZAR TEMP_PROCESS EN LA CELDA 1
notification_raised(webhook_url, 0, "DONE", source, input_parameters)

# COMMAND ----------

# DBTITLE 1,Eliminación Tablas Delta
from pyspark.sql.functions import col

Tablas_Persistentes = [f"oracle_dispersiones_{sr_folio}"]

# List delta tables with the specified folio
tables = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}") \
    .filter(col("tableName").endswith(sr_folio))\
    .filter(~col("tableName").isin(Tablas_Persistentes))

# Drop the tables with the specified folio
for row in tables.collect():
    table_name = row.tableName
    try: 
        spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.{schema_name}.{table_name}")
        print(f"tabla {table_name} borrada")
    except:
        print(f"error al borrar: {table_name}") 



# Forzar la recolección de basura
import gc
gc.collect()

print("Proceso de eliminación de tablas y liberación de memoria completado.")
