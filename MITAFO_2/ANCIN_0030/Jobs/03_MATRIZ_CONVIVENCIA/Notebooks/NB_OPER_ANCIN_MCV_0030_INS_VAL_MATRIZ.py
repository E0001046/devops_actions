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
# MAGIC         sql_conf_file = config.get(process_name, "sql_conf_file_abono")
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
# MAGIC             # Unity
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
# MAGIC         scope,
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

# DBTITLE 1,JP_PANCIN_MCV_0030_VALIDA_CONV_ABONO_01
query_statement = '002' #ORACLE

params = [sr_folio]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends") 


df, failed_task = query_table(conn_name_ora, spark, statement,  conn_options, conn_user, conn_key)

df.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog_name}.{schema_name}.{'DF_Job030_01' + '_' + sr_folio}"
)
#df.createOrReplaceTempView("DF_Job030_01")

if debug:
    print(statement)
    df.printSchema()
    display(df)

# COMMAND ----------

# DBTITLE 1,JP_PANCIN_MCV_0030_VALIDA_CONV_ABONO_02
query_statement = '003' #DATABRICKS (vistas temp)

params = [sr_folio, catalog_name, schema_name]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends") 

#Ejecuta la consulta del lado de databricks
df = spark.sql(statement)

#df, failed_task = query_table(conn_name_ora, spark, statement,  conn_options, conn_user, conn_key)
df.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog_name}.{schema_name}.{'TEMP_VALIDA_DISP_MATRIZ' + '_' + sr_folio}"
)
#df.createOrReplaceTempView("TEMP_VALIDA_DISP_MATRIZ")

if debug:
    print(statement)
    df.printSchema()
    display(df)

# COMMAND ----------

# DBTITLE 1,JP_PANCIN_MCV_0030_VALIDA_CONV_ABONO_03
query_statement = '004'

params = []

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends") 


df, failed_task = query_table(conn_name_ora, spark, statement,  conn_options, conn_user, conn_key)
df.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog_name}.{schema_name}.{'DF_Job030_02' + '_' + sr_folio}"
)
#df.createOrReplaceTempView("DF_Job030_02")

if debug:
    df.printSchema()
    display(df)

# COMMAND ----------

# DBTITLE 1,JP_PANCIN_MCV_0040_GEN_MATRIZ_ABONO_01
from pyspark.sql.functions import col

query_statement = '005'

params = [sr_folio, catalog_name, schema_name]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends") 

#Ejecuta la consulta del lado de databricks
df = spark.sql(statement)
df = df.withColumn('FTN_ID_ERROR_VAL', col('FTN_ID_ERROR_VAL').cast('int'))
df.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog_name}.{schema_name}.{'TEMP_VAL_MATRIZ' + '_' + sr_folio}"
)
#df.createOrReplaceTempView("TEMP_VAL_MATRIZ")

if debug:
    print(statement)
    df.printSchema()
    display(df)


# COMMAND ----------

# DBTITLE 1,JP_PANCIN_MCV_0040_GEN_MATRIZ_ABONO_02
query_statement = "006"

params = [sr_tipo_mov, catalog_name, schema_name, sr_folio]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == "0":
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

df = spark.sql(statement)

# Ejecuta la consulta del lado de databricks
df.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog_name}.{schema_name}.{'VAL_MATRIZ_05' + '_' + sr_folio}"
)
# df = spark.sql(statement)
# df.createOrReplaceTempView("VAL_MATRIZ_05")
df = df.distinct()

if debug:
    print(statement)
    df.printSchema()
    display(df)

# COMMAND ----------

from pyspark.sql.functions import col, when

# Crear una columna auxiliar que le dé prioridad a las filas donde FTC_ESTATUS_MARCA = 0
df = df.withColumn("prioridad", when(col("FTC_ESTATUS_MARCA") == 0, 1).otherwise(0))

# Ordenar por FTN_NUM_CTA_INVDUAL y la nueva columna "prioridad" para que las filas con FTC_ESTATUS_MARCA = 0 estén primero
df_windowed = df.orderBy(col("FTN_NUM_CTA_INVDUAL"), col("prioridad").desc())

# Eliminar duplicados basados en FTN_NUM_CTA_INVDUAL, manteniendo solo el registro con FTC_ESTATUS_MARCA = 0 si existe
df_final = df_windowed.dropDuplicates(["FTN_NUM_CTA_INVDUAL"])

# Mostrar el resultado final
df = df_final

df = df.withColumnRenamed("FTF_MONTO", "FTN_MONTO")

if debug:
    print(statement)
    df.printSchema()
    display(df)


# COMMAND ----------

df = df.drop("prioridad")

# COMMAND ----------

# DBTITLE 1,INSERT CIERREN_ETL.TLSISGRAL_ETL_VAL_MATRIZ_CONV_AUX
#if sr_reproceso != '0':
    #se realizará el insert a CIERREN_ETL.TLSISGRAL_ETL_VAL_MATRIZ_CONV mediante una tabla auxliar ya que es necesario ejecutar una secuencia para establecer el ID_MARCA

mode = 'APPEND'

table_name_001 = 'CIERREN_DATAUX.TLSISGRAL_ETL_VAL_MATRIZ_CONV_AUX'
    
failed_task = write_into_table(conn_name_ora, df, mode, table_name_001, conn_options, conn_aditional_options, conn_user, conn_key)

if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")

# COMMAND ----------

# DBTITLE 1,DELETE VAL_MATRIZ_CONV PART 1
#delete

query_statement = '010'

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
spark.conf.set("scope", str(scope))

# COMMAND ----------

# DBTITLE 1,DELETE VAL_MATRIZ_CONV PART 2
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
# MAGIC var connection: java.sql.Connection = null
# MAGIC
# MAGIC try {
# MAGIC     connectionProperties.setProperty("user", dbutils.secrets.get(scope=conn_scope, key=conn_user))
# MAGIC     connectionProperties.setProperty("password", dbutils.secrets.get(scope=conn_scope, key=conn_key))
# MAGIC     connectionProperties.setProperty("v$session.osuser", dbutils.secrets.get(scope=conn_scope, key=conn_user))
# MAGIC
# MAGIC     connection = DriverManager.getConnection(conn_url, connectionProperties)
# MAGIC     val stmt = connection.createStatement()
# MAGIC     val sql = spark.conf.get("statement")
# MAGIC
# MAGIC     stmt.execute(sql)
# MAGIC     spark.conf.set("failed_task", "1")
# MAGIC } catch {
# MAGIC     case _: Throwable => spark.conf.set("failed_task", "0")
# MAGIC } finally {
# MAGIC     if (connection != null) {
# MAGIC         connection.close()
# MAGIC     }
# MAGIC }

# COMMAND ----------

# DBTITLE 1,DELETE VAL_MATRIZ_CONV PART 3
#Manejo de errores de la operación realizada en Scala
failed_task = spark.conf.get("failed_task")

if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")


# COMMAND ----------

# DBTITLE 1,INSERT CIERREN_ETL.TLSISGRAL_ETL_VAL_MATRIZ_CONV Part1
#Ejecuta Merge para insersión en la tabla CIERREN_ETL.TLSISGRAL_ETL_VAL_MATRIZ_CONV

statement = f"""MERGE INTO CIERREN_ETL.TLSISGRAL_ETL_VAL_MATRIZ_CONV ori
USING (select * FROM CIERREN_DATAUX.TLSISGRAL_ETL_VAL_MATRIZ_CONV_AUX aux where aux.FTC_FOLIO = '{sr_folio}'
            AND aux.FCN_ID_PROCESO = {sr_proceso}
            AND aux.FCN_ID_SUBPROCESO = {sr_subproceso}) aux
    ON (
            ori.FTN_NUM_CTA_INVDUAL = aux.FTN_NUM_CTA_INVDUAL
            AND ori.FTC_FOLIO = aux.FTC_FOLIO
            AND ori.FCN_ID_TIPO_SUBCTA = aux.FCN_ID_TIPO_SUBCTA
            )
WHEN MATCHED
    THEN
        UPDATE
        SET ori.FTF_MONTO = aux.FTN_MONTO
            ,ori.FTB_ESTATUS_MARCA = aux.FTC_ESTATUS_MARCA
            ,ori.FCN_ID_TIPO_MONTO = aux.FCN_ID_TIPO_MONTO
            ,ori.FTD_FECHA = aux.FTD_FECHA
            ,ori.FTN_ID_SUBPROC_NO_CONV = aux.FTN_ID_SUBPROC_NO_CONV
            ,ori.FTN_ID_ERROR_VAL = aux.FTN_ID_ERROR_VAL
            ,ori.FTC_FOLIO_REL = aux.FTC_FOLIO_REL
            ,ori.FTN_ID_MARCA = aux.FTN_ID_MARCA

WHEN NOT MATCHED
    THEN
        INSERT (
            FTC_FOLIO
            ,FCN_ID_PROCESO
            ,FCN_ID_SUBPROCESO
            ,FTN_NUM_CTA_INVDUAL
            ,FTF_MONTO
            ,FCN_ID_TIPO_SUBCTA
            ,FTB_ESTATUS_MARCA
            ,FCN_ID_TIPO_MONTO
            ,FTD_FECHA
            ,FTN_ID_SUBPROC_NO_CONV
            ,FTN_ID_ERROR_VAL
            ,FTC_FOLIO_REL
            ,FTN_ID_MARCA

            )
        VALUES (
            aux.FTC_FOLIO
            ,aux.FCN_ID_PROCESO
            ,aux.FCN_ID_SUBPROCESO
            ,aux.FTN_NUM_CTA_INVDUAL
            ,aux.FTN_MONTO
            ,aux.FCN_ID_TIPO_SUBCTA
            ,aux.FTC_ESTATUS_MARCA
            ,aux.FCN_ID_TIPO_MONTO
            ,aux.FTD_FECHA
            ,aux.FTN_ID_SUBPROC_NO_CONV
            ,aux.FTN_ID_ERROR_VAL
            ,aux.FTC_FOLIO_REL
            ,CIERREN.SE_TTAFOGRAL_MARCA_CTA_SUBCTA.nextval
            )"""

print(statement)

spark.conf.set("conn_url", str(conn_url))
spark.conf.set("conn_user", str(conn_user))
spark.conf.set("conn_key", str(conn_key))
spark.conf.set("statement", str(statement))
spark.conf.set("scope", str(scope))


# COMMAND ----------

# DBTITLE 1,INSERT CIERREN_ETL.TLSISGRAL_ETL_VAL_MATRIZ_CONV Part2
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
# MAGIC var connection: java.sql.Connection = null
# MAGIC
# MAGIC try {
# MAGIC     connectionProperties.setProperty("user", dbutils.secrets.get(scope=conn_scope, key=conn_user))
# MAGIC     connectionProperties.setProperty("password", dbutils.secrets.get(scope=conn_scope, key=conn_key))
# MAGIC     connectionProperties.setProperty("v$session.osuser", dbutils.secrets.get(scope=conn_scope, key=conn_user))
# MAGIC
# MAGIC     connection = DriverManager.getConnection(conn_url, connectionProperties)
# MAGIC     val stmt = connection.createStatement()
# MAGIC     val sql = spark.conf.get("statement")
# MAGIC
# MAGIC     stmt.execute(sql)
# MAGIC     spark.conf.set("failed_task", "1")
# MAGIC } catch {
# MAGIC     case _: Throwable => spark.conf.set("failed_task", "0")
# MAGIC } finally {
# MAGIC     if (connection != null) {
# MAGIC         connection.close()
# MAGIC     }
# MAGIC }

# COMMAND ----------

# DBTITLE 1,INSERT CIERREN_ETL.TLSISGRAL_ETL_VAL_MATRIZ_CONV Part3
#Manejo de errores de la operación realizada en Scala
failed_task = spark.conf.get("failed_task")

if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")

# COMMAND ----------

# DBTITLE 1,Limpia Tabla Auxiliar Part1
statement = f"""
DELETE FROM CIERREN_DATAUX.TLSISGRAL_ETL_VAL_MATRIZ_CONV_AUX
WHERE FTC_FOLIO = '{sr_folio}'
"""
print(statement)

spark.conf.set("conn_url", str(conn_url))
spark.conf.set("conn_user", str(conn_user))
spark.conf.set("conn_key", str(conn_key))
spark.conf.set("statement", str(statement))
spark.conf.set("scope", str(scope))


# COMMAND ----------

# DBTITLE 1,Limpia Tabla Auxiliar Part2
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
# MAGIC var connection: java.sql.Connection = null
# MAGIC
# MAGIC try {
# MAGIC     connectionProperties.setProperty("user", dbutils.secrets.get(scope=conn_scope, key=conn_user))
# MAGIC     connectionProperties.setProperty("password", dbutils.secrets.get(scope=conn_scope, key=conn_key))
# MAGIC     connectionProperties.setProperty("v$session.osuser", dbutils.secrets.get(scope=conn_scope, key=conn_user))
# MAGIC
# MAGIC     connection = DriverManager.getConnection(conn_url, connectionProperties)
# MAGIC     val stmt = connection.createStatement()
# MAGIC     val sql = spark.conf.get("statement")
# MAGIC
# MAGIC     stmt.execute(sql)
# MAGIC     spark.conf.set("failed_task", "1")
# MAGIC } catch {
# MAGIC     case _: Throwable => spark.conf.set("failed_task", "0")
# MAGIC } finally {
# MAGIC     if (connection != null) {
# MAGIC         connection.close()
# MAGIC     }
# MAGIC }

# COMMAND ----------

# DBTITLE 1,Limpia Tabla Auxiliar Part3
#Manejo de errores de la operación realizada en Scala
failed_task = spark.conf.get("failed_task")

if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")
