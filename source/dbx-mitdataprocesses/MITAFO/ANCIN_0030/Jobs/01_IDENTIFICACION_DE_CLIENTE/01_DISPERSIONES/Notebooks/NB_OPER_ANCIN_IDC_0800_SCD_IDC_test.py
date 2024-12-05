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
# MAGIC from pyspark.sql.functions import length, lit, expr, nvl, to_date
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
# MAGIC         debug = config.get(process_name, 'debug')
# MAGIC         debug = debug.lower() == 'true'
# MAGIC  
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC         return '0','0','0'
# MAGIC     return sql_conf_file,debug, '1'
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
# MAGIC     sql_conf_file,debug, failed_task = conf_process_values(config_process_file, process_name)
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

# DBTITLE 1,CONSTRUCCIÓN DE LA CONSULTA SQL
query_statement = '010'

params = [sr_id_archivo]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

#print(statement)
df = spark.sql(statement)


if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")
    
print(debug)    
if debug:
    display(df)    


# COMMAND ----------

# DBTITLE 1,FILTRADO DE INFORMACIÓN BANDERA NULA
df_nulos = df.filter("FLAG IS NULL")
df_nulos = df_nulos.select(
"FTC_FOLIO",
"FTN_ID_ARCHIVO",
"FTN_NSS",
"FTC_CURP",
"FTC_NOMBRE_CTE",
"FTN_NUM_CTA_INVDUAL",
"FTN_ESTATUS_DIAG",
nvl(df_nulos.FTC_ID_DIAGNOSTICO,lit('')).alias("FTC_ID_DIAGNOSTICO"),
nvl(df_nulos.FTC_ID_SUBP_NO_VIG,lit('')).alias("FTC_ID_SUBP_NO_VIG"),
to_date(df_nulos.FTD_FECHA_CERTIFICACION, 'dd/MM/yyyy').alias("FTD_FECHA_CERTIFICACION"),
"FTN_CTE_PENSIONADO"
)

print(debug)    
if debug:
    display(df_nulos)    

# COMMAND ----------

# DBTITLE 1,INSERCIÓN DE REGISTROS A LA TABLA ETL_VAL_IDENT_CTE
#INSERCIÓN tabla CIERREN_ETL.TTSISGRAL_ETL_DISPERSION_CTAS
#has_data = df_nulos.head(1)

if not df_nulos.isEmpty():
    mode = 'APPEND'

    table_name_001 = 'RECADOIMSSADM.TTSISGRAL_ETL_VAL_IDENT_CTE'
    #CIERREN.TLAFOGRAL_SUMARIO_ARCHIVO
    failed_task = write_into_table(conn_name_ora, df_nulos, mode, table_name_001, conn_options, conn_aditional_options, conn_user, conn_key)

    if failed_task == '0':
        logger.error("Please review log messages")
        notification_raised(webhook_url, -1, message, source, input_parameters)
        raise Exception("An error raised")


# COMMAND ----------

# DBTITLE 1,FILTRADO DE INFORMACIÓN BANDERA NO NULA

from pyspark.sql.functions import col


df_nonulos = df.where(
    (col("FLAG").isNotNull()) &
    (col("FTN_NUM_CTA_INVDUAL") != col("FTN_NUM_CTA_INVDUAL_2")) |
    (col("FTN_ESTATUS_DIAG") != col("FTN_ESTATUS_DIAG_2")) |
    (col("FTC_ID_DIAGNOSTICO") != col("FTC_ID_DIAGNOSTICO_2")) |
    (col("FTC_ID_SUBP_NO_VIG") != col("FTC_ID_SUBP_NO_VIG_2")) |
    (col("FTN_CTE_PENSIONADO") != col("FTN_CTE_PENSIONADO_2")) |
    (col("FTD_FECHA_CERTIFICACION") != col("FTD_FECHA_CERTIFICACION_2"))
).select(
    "FTC_FOLIO",
    "FTN_NSS",
    "FTC_CURP",
    "FTN_NUM_CTA_INVDUAL",
    "FTN_ESTATUS_DIAG",
    "FTC_ID_DIAGNOSTICO",
    "FTC_ID_SUBP_NO_VIG",
    "FTN_CTE_PENSIONADO"
)

#df_nonulos = df.select(
#"FTC_FOLIO",
#"FTN_NSS",
#"FTC_CURP",
#"FTN_NUM_CTA_INVDUAL",
#"FTN_ESTATUS_DIAG",
#"FTC_ID_DIAGNOSTICO",
#"FTC_ID_SUBP_NO_VIG",
#"FTN_CTE_PENSIONADO"
#)

print(debug)    
if debug:
    display(df_nonulos)    

# COMMAND ----------

# DBTITLE 1,VALIDACIÓN DEL ESQUEMA DEL df_nonulos
#df_nonulos.printSchema()

# COMMAND ----------

# DBTITLE 1,CAMBIO DE TIPO DE DATO FTC_ID_SUBP_NO_VIG
from pyspark.sql.functions import col

df_nonulos = df_nonulos.withColumn("FTC_ID_SUBP_NO_VIG", col("FTC_ID_SUBP_NO_VIG").cast("decimal(10,0)"))

print(debug)    
if debug:
    display(df_nonulos)    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cambiamos los campos y propiedas del df_nonulos para poder insertarlo en la tabla auxiliar.

# COMMAND ----------

from pyspark.sql.functions import col

df_nonulos = df_nonulos.select(
    col("FTC_FOLIO").alias("FTC_FOLIO"),
    col("FTN_NSS").alias("FTC_NSS"),
    col("FTC_CURP").alias("FTC_CURP"),
    col("FTN_NUM_CTA_INVDUAL").alias("FTN_NUM_CTA_INVDUAL"),
    col("FTN_ESTATUS_DIAG").alias("FTN_ESTATUS_DIAG"),
    col("FTC_ID_DIAGNOSTICO").alias("FTN_ID_DIAGNOSTICO"),
    col("FTC_ID_SUBP_NO_VIG").alias("FTN_ID_SUBP_NO_VIG"),
    col("FTN_CTE_PENSIONADO").alias("FTN_CTE_PENSIONADO")
)

#df_nonulos.count()

# Mostrar el DataFrame con los nuevos nombres de columnas
if debug:
    display(df_nonulos)

# COMMAND ----------

# DBTITLE 1,LLENADO DE LA TABLA AUXILIAR PARA UPDATE
#INSERCIÓN tabla CIERREN_ETL.TTSISGRAL_ETL_DISPERSION_CTAS
mode = 'APPEND'

table_name_001 = 'RECADOIMSSADM.TTSISGRAL_ETL_VAL_IDENT_CTE_AUX'
#CIERREN.TLAFOGRAL_SUMARIO_ARCHIVO
failed_task = write_into_table(conn_name_ora, df_nonulos, mode, table_name_001, conn_options, conn_aditional_options, conn_user, conn_key)

if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")

# COMMAND ----------

# DBTITLE 1,INVOCACIÓN DE MERGE PARA UPDATE
query_statement = '022'

params = [0]

statement, failed_task = getting_statement(conf_values, query_statement, params)
statement = """
MERGE INTO RECADOIMSSADM.TTSISGRAL_ETL_VAL_IDENT_CTE ori
              USING (
              SELECT 
              FTC_FOLIO
              FTN_NSS
              FTC_CURP
              FTN_NUM_CTA_INVDUAL
              FTN_ESTATUS_DIAG
              FTC_ID_DIAGNOSTICO
              FTC_ID_SUBP_NO_VIG
              FTN_CTE_PENSIONADO
                  FROM (
                      SELECT 
              FTC_FOLIO FTC_FOLIO
              FTC_NSS FTN_NSS
              FTC_CURP FTC_CURP
              FTN_NUM_CTA_INVDUAL FTN_NUM_CTA_INVDUAL
              FTN_ESTATUS_DIAG FTN_ESTATUS_DIAG
              FTN_ID_DIAGNOSTICO FTC_ID_DIAGNOSTICO
              FTN_ID_SUBP_NO_VIG FTC_ID_SUBP_NO_VIG
              FTN_CTE_PENSIONADO FTN_CTE_PENSIONADO
                          ROW_NUMBER() OVER (PARTITION BY FTC_FOLIO ORDER BY FTC_FOLIO) AS rn
                      FROM RECADOIMSSADM.TTSISGRAL_ETL_VAL_IDENT_CTE_AUX
                  ) subquery
                  WHERE rn = 1
              ) aux 
              ON (ori.FTC_FOLIO = aux.FTC_FOLIO
                  AND ori.FTN_NSS = aux.FTN_NSS)
              WHEN MATCHED THEN
                  UPDATE SET
                      ori.FTC_CURP = aux.FTC_CURP
              ori.FTN_NUM_CTA_INVDUAL = aux.FTN_NUM_CTA_INVDUAL
              ori.FTN_ESTATUS_DIAG = aux.FTN_ESTATUS_DIAG
              ori.FTC_ID_DIAGNOSTICO = aux.FTC_ID_DIAGNOSTICO
              ori.FTC_ID_SUBP_NO_VIG = aux.FTC_ID_SUBP_NO_VIG
              ori.FTN_CTE_PENSIONADO = aux.FTN_CTE_PENSIONADO
"""
if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")
conn_options : {"url":"jdbc:oracle:thin:username/password@//74.249.129.242:1521/RECADOIMSS", "driver":"oracle.jdbc.driver.OracleDriver", "fetchSize": "5000", "numPartitions": "15", "queryTimeout": "0", "connectRetryCount" : "10", "connectRetryInterval" : "10"}
#scope : Scope_dev_kv
conn_user : RECADOIMSSADM
conn_key : RECADOIMSS123
conn_driver : oracle.jdbc.driver.OracleDriver

df, failed_task = query_table(conn_name_ora, spark, statement,  conn_options, conn_user, conn_key)

if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")
    

# COMMAND ----------

statement

# COMMAND ----------

# DBTITLE 1,SE CARGAN LAS VARIABLES
spark.conf.set("conn_url", str(conn_url))
spark.conf.set("conn_user", str(conn_user))
spark.conf.set("conn_key", str(conn_key))
spark.conf.set("statement", str(statement))

# COMMAND ----------

# DBTITLE 1,SE ENVIA EL MERGE CON SCALA
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
# MAGIC stmt.execute(sql)
# MAGIC connection.close()

# COMMAND ----------

# DBTITLE 1,SE DEPURA LA TABLA AUXILIAR
statement = f"""
DELETE FROM RECADOIMSSADM.TTSISGRAL_ETL_VAL_IDENT_CTE_AUX
WHERE FTC_FOLIO = '{sr_folio}'
"""
statement

# COMMAND ----------

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
# MAGIC stmt.execute(sql)
# MAGIC connection.close()
