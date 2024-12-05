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
# MAGIC import uuid
# MAGIC
# MAGIC from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
# MAGIC from pyspark.sql.functions import length, lit
# MAGIC
# MAGIC def input_values():
# MAGIC     """ Recupera valores de entrada desde widgets.
# MAGIC     Returns:
# MAGIC         tuple: Una tupla que contiene los valores de entrada y un indicador de estado.
# MAGIC     """
# MAGIC     try:
# MAGIC         # Define widgets de texto en Databricks para capturar valores de entrada.
# MAGIC         dbutils.widgets.text('sr_folio', '')
# MAGIC         dbutils.widgets.text('sr_proceso', '')
# MAGIC         dbutils.widgets.text('sr_subproceso', '')
# MAGIC         dbutils.widgets.text('sr_subetapa', '')
# MAGIC         dbutils.widgets.text('sr_tipo_mov', '')
# MAGIC         dbutils.widgets.text('sr_accion', '')
# MAGIC         dbutils.widgets.text('sr_instancia_proceso', '')
# MAGIC         dbutils.widgets.text('sr_usuario', '')
# MAGIC         dbutils.widgets.text('sr_etapa', '')
# MAGIC         dbutils.widgets.text('sr_id_snapshot', '')
# MAGIC
# MAGIC         # Recupera los valores de los widgets definidos anteriormente.
# MAGIC         sr_folio = dbutils.widgets.get('sr_folio')
# MAGIC         sr_proceso = dbutils.widgets.get('sr_proceso')
# MAGIC         sr_subproceso = dbutils.widgets.get('sr_subproceso')
# MAGIC         sr_subetapa = dbutils.widgets.get('sr_subetapa')
# MAGIC         sr_tipo_mov = dbutils.widgets.get('sr_tipo_mov')
# MAGIC         sr_accion = dbutils.widgets.get('sr_accion')
# MAGIC         sr_instancia_proceso = dbutils.widgets.get('sr_instancia_proceso')
# MAGIC         sr_usuario = dbutils.widgets.get('sr_usuario')
# MAGIC         sr_etapa = dbutils.widgets.get('sr_etapa')
# MAGIC         sr_id_snapshot = dbutils.widgets.get('sr_id_snapshot')
# MAGIC
# MAGIC         # Verifica si alguno de los valores recuperados está vacío o es nulo.
# MAGIC         # Si algún valor está vacío, registra un error y retorna una tupla de ceros.
# MAGIC         if any(len(str(value).strip()) == 0 for value in [sr_folio, sr_proceso, sr_subproceso, sr_subetapa, sr_tipo_mov, sr_accion, sr_instancia_proceso, sr_usuario, sr_etapa, sr_id_snapshot]):
# MAGIC             logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC             logger.error("Some of the input values are empty or null")
# MAGIC             return '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'
# MAGIC     
# MAGIC     # Captura cualquier excepción que ocurra durante la ejecución del bloque try.
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: %s", str(e))
# MAGIC         return '0', '0', '0', '0', '0', '0', '0', '0', '0', '0','0'
# MAGIC     return sr_folio, sr_proceso, sr_subproceso, sr_subetapa, sr_tipo_mov, sr_accion, sr_instancia_proceso, sr_usuario, sr_etapa, sr_id_snapshot, '1'
# MAGIC
# MAGIC
# MAGIC def conf_process_values(config_file, process_name):
# MAGIC     try:
# MAGIC         # Inicializa un objeto ConfigParser y lee el archivo de configuración especificado por config_file.
# MAGIC         config = configparser.ConfigParser()
# MAGIC         config.read(config_file)
# MAGIC
# MAGIC         # Configuración de subprocesos.
# MAGIC         # Recupera los valores de configuración específicos para el proceso indicado por process_name.
# MAGIC         sql_conf_file = config.get(process_name, 'sql_conf_file')
# MAGIC     # Captura cualquier excepción que ocurra durante la ejecución del bloque try.
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC         return '0', '0'
# MAGIC     return sql_conf_file, '1'
# MAGIC
# MAGIC # El código principal se ejecuta cuando el script se ejecuta directamente (no cuando se importa como un módulo).
# MAGIC if __name__ == "__main__":
# MAGIC     logging.basicConfig()
# MAGIC     logger = logging.getLogger(__name__)
# MAGIC     logger.setLevel(logging.DEBUG)
# MAGIC     notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
# MAGIC     message = 'NB Error: ' + notebook_name
# MAGIC     source = 'ETL'
# MAGIC     root_repo = '/Workspace/Shared/MITAFO'
# MAGIC     
# MAGIC     # Intenta importar funciones adicionales desde otros notebooks. Si ocurre un error, lo registra.
# MAGIC     try:
# MAGIC         sys.path.append(root_repo + '/' + 'CGRLS_0010/Notebooks')
# MAGIC         from NB_GRLS_DML_FUNCTIONS import *
# MAGIC         from NB_GRLS_SIMPLE_FUNCTIONS import *
# MAGIC     except Exception as e:
# MAGIC         logger.error("Error at the beggining of the process")
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC
# MAGIC     # Define las rutas de los archivos de configuración necesarios para el proceso.
# MAGIC     config_file = root_repo + '/' + 'CGRLS_0010/Conf/CF_GRLS_PROCESS.py.properties'
# MAGIC     config_conn_file = root_repo + '/' + 'CGRLS_0010/Conf/CF_GRLS_CONN.py.properties'
# MAGIC     config_process_file = root_repo + '/' + 'ANCIN_0030/Jobs/05_CONCLUSION/03_DESMAR_CUENTA/Conf/CF_PART_PROC.py.properties'
# MAGIC
# MAGIC     # Llama a la función input_values para recuperar los valores de entrada. Si hay un error, registra el mensaje y lanza una excepción.
# MAGIC     sr_folio, sr_proceso, sr_subproceso, sr_subetapa, sr_tipo_mov, sr_accion, sr_instancia_proceso, sr_usuario, sr_etapa, sr_id_snapshot,failed_task = input_values()
# MAGIC     
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         #notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("An error ocurred, check out log messages")
# MAGIC
# MAGIC     # Obtiene todos los parámetros de entrada desde los widgets de Databricks.
# MAGIC     input_parameters = dbutils.widgets.getAll().items()
# MAGIC     
# MAGIC     # Llama a la función conf_init_values para recuperar los valores de configuración inicial. Si hay un error, registra el mensaje, envía una notificación y lanza una excepción.
# MAGIC     process_name = 'root'
# MAGIC     webhook_url, channel, failed_task = conf_init_values(config_file, process_name, 'DESMARCA')
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     # Llama a la función conf_process_values para recuperar los valores de configuración del proceso. Si hay un error, registra el mensaje, envía una notificación y lanza una excepción.
# MAGIC     process_name = 'root'
# MAGIC     sql_conf_file, failed_task = conf_process_values(config_process_file, process_name)
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     # Llama a la función conf_conn_values para recuperar los valores de configuración de conexión. Si hay un error, registra el mensaje, envía una notificación y lanza una excepción.
# MAGIC     conn_name_ora = 'jdbc_oracle'
# MAGIC     conn_options, conn_aditional_options, conn_user, conn_key, conn_url, scope, failed_task = conf_conn_values(config_conn_file, conn_name_ora)
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     # Definición de la ruta del archivo de configuración SQL.
# MAGIC     sql_conf_file = root_repo + '/' + '/ANCIN_0030/Jobs/05_CONCLUSION/03_DESMAR_CUENTA/JSON/' + sql_conf_file

# COMMAND ----------

# DBTITLE 1,CONFIGURACIÓN ARCHIVO SQL Y JSON
# Abre el archivo de configuración SQL y carga su contenido en formato JSON
with open(sql_conf_file) as f:
    file_config_sql = json.load(f)

# Procesa el contenido del archivo JSON para extraer los valores de configuración
# Crea una lista de tuplas donde cada tupla contiene el 'step_id' y los valores concatenados por salto de línea
conf_values = [
    (fields['step_id'], '\n'.join(fields['value']))  # Crea una tupla con 'step_id' y los valores concatenados
    for line, value in file_config_sql.items() if line == 'steps'  # Itera sobre los elementos del JSON y filtra por 'steps'
    for fields in value  # Itera sobre los campos dentro de 'steps'
]

# COMMAND ----------

# DBTITLE 1,VALIDACIÓN DE LOGITUD FOLIO
from pyspark.sql.functions import col, length, substring, when

# Crea un DataFrame con el valor de sr_folio
val_folio = spark.createDataFrame([(sr_folio,)], ["sr_folio"])

# Evalúa la condición y crea la columna val_folio
val_folio = val_folio.withColumn("val_folio", when(
    (length(col("sr_folio")) == 18) &  # Verifica si la longitud de sr_folio es 18
    (substring(col("sr_folio"), 1, 4) > '2016') &  # Verifica si los primeros 4 caracteres de sr_folio son mayores a '2016'
    (substring(col("sr_folio"), 1, 4) < '3017'),  # Verifica si los primeros 4 caracteres de sr_folio son menores a '3017'
    1  # Si todas las condiciones se cumplen, asigna 1 a val_folio
).otherwise(0))  # Si alguna condición no se cumple, asigna 0 a val_folio

# Muestra el DataFrame
#display(val_folio)

# COMMAND ----------

# DBTITLE 1,CONSULTA DESMARCA FOLIO O INSTANCIA
import sys

valor_folio = int(val_folio.select('val_folio').first()['val_folio'])

#SR_ACCION 0 = Desmarca matriz

# Arma la consulta para desmarcar el folio
if valor_folio == 1 and sr_accion == "0":
    query_statement = "001"
    params = [sr_accion, sr_folio]

    # Obtiene la declaración y verifica si hubo un error
    statement, failed_task = getting_statement(conf_values, query_statement, params)

    if failed_task == "0":
        logger.error("No value %s found", statement)
        logger.error("Please review log messages")
        notification_raised(webhook_url, -1, message, source, input_parameters)
        raise Exception("Process ends")

    # Consulta la tabla y verifica si hubo un error
    df_desmarca, failed_task = query_table(
        conn_name_ora, spark, statement, conn_options, conn_user, conn_key
    )

    if failed_task == "0":
        logger.error("Please review log messages")
        notification_raised(webhook_url, -1, message, source, input_parameters)
        raise Exception("An error raised")

# Arma la consulta para desmarcar la instancia proceso AVOL
elif valor_folio == 0 and sr_accion == "0":
    query_statement = "002"
    params = [sr_folio]

    # Obtiene la declaración y verifica si hubo un error
    statement, failed_task = getting_statement(conf_values, query_statement, params)

    if failed_task == "0":
        logger.error("No value %s found", statement)
        logger.error("Please review log messages")
        notification_raised(webhook_url, -1, message, source, input_parameters)
        raise Exception("Process ends")

    # Consulta la tabla y verifica si hubo un error
    df_desmarca, failed_task = query_table(
        conn_name_ora, spark, statement, conn_options, conn_user, conn_key
    )

    if failed_task == "0":
        logger.error("Please review log messages")
        notification_raised(webhook_url, -1, message, source, input_parameters)
        raise Exception("An error raised")

else:
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised, por favor validar que los parámetros de entrada sean los correctos")
    # sys.exit("Parametros incorrectos")

# COMMAND ----------

# DBTITLE 1,RENOMBRE DE CAMPOS PARA LLENAR AUX
# Renombre de campos para que puedan ser insertados en la tabla Aux 
from pyspark.sql.functions import col, lit

df_desmarca = df_desmarca.select(
    col("FTC_FOLIO").alias("FTC_FOLIO"),
    col("FTN_ID_MARCA").alias("FTN_ID_MARCA"),
    col("FTB_ESTATUS_MARCA").alias("FTC_ESTATUS_MARCA"),
    col("FTD_FEH_ACT").alias("FTD_FEH_ACT"),
    col("FCC_USU_ACT").alias("FCC_USU_ACT"),
    lit(1).alias("FTN_NUM_CTA_INVDUAL")  # Nueva columna con valor 1, sólo se agrega para llenar la tabla auxiliar pero no se require para la desmarca.
)

display(df_desmarca)

# COMMAND ----------

# DBTITLE 1,LLENADO DE TABLA AUXILIAR PARA UPDATE
# INSERCIÓN DE DATOS A TABLA AUXILIAR CIERREN_DATAUX.TTAFOGRAL_ETL_MATRIZ_CONVIVENCIA_AUX
# Esta tabla se comparte con la subetapa de Matriz de Convivencia

mode = 'APPEND'  # Modo de inserción: 'APPEND' para agregar datos sin borrar los existentes

table_name_001 = 'CIERREN_DATAUX.TTAFOGRAL_ETL_MATRIZ_CONVIVENCIA_AUX'  # Nombre de la tabla auxiliar

# Escribe los datos en la tabla auxiliar y verifica si hubo un error
failed_task = write_into_table(conn_name_ora, df_desmarca, mode, table_name_001, conn_options, conn_aditional_options, conn_user, conn_key)

# Si la tarea falló, registra el error y lanza una excepción
if failed_task == '0':
    logger.error("Please review log messages")  # Registra un mensaje de error
    notification_raised(webhook_url, -1, message, source, input_parameters)  # Envía una notificación de error
    raise Exception("An error raised")  # Lanza una excepción indicando que ocurrió un error

# COMMAND ----------

# DBTITLE 1,INVOCACIÓN DE MERGE PARA UPDATE
# Define el identificador de la consulta
query_statement = '003'

# Parámetros para la consulta
params = [0]

# Recupera la declaración SQL usando el identificador y los parámetros
statement, failed_task = getting_statement(conf_values, query_statement, params)

# Verifica si la recuperanción de la declaración SQL falló.
if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

# Ejecuta la consulta y recupera el resultado como un DataFrame
df, failed_task = query_table(conn_name_ora, spark, statement, conn_options, conn_user, conn_key)

# Verifica si la ejecución de la consulta falló.
if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")

# COMMAND ----------

# DBTITLE 1,SETEO DE VARIABLES SPARK
# Establece la URL de conexión en la configuración de Spark
spark.conf.set("conn_url", str(conn_url))

# Establece el usuario de conexión en la configuración de Spark
spark.conf.set("conn_user", str(conn_user))

# Establece la clave de conexión en la configuración de Spark
spark.conf.set("conn_key", str(conn_key))

# Establece la declaración SQL en la configuración de Spark
spark.conf.set("statement", str(statement))

# Establece el scope de conexión en la configuración de Spark
spark.conf.set("scope", str(scope))

# COMMAND ----------

# DBTITLE 1,ENVÍO DE MERGE CON SCALA
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

#Manejo de errores de la operación realizada en Scala

failed_task = spark.conf.get("failed_task")

if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")

# COMMAND ----------

# DBTITLE 1,DEFINICIÓN DELETE TABLA AUXILIAR
# Definir la sentencia SQL de DELETE con el folio específico
statement = f"""
DELETE FROM CIERREN_DATAUX.TTAFOGRAL_ETL_MATRIZ_CONVIVENCIA_AUX
WHERE FTC_FOLIO = '{sr_folio}'
"""
# Mostrar la sentencia SQL generada
statement

# COMMAND ----------

# DBTITLE 1,SENTENCIA DELETE
# Establece la sentencia SQL de DELETE en la configuración de Spark
spark.conf.set("statement", str(statement))

# COMMAND ----------

# DBTITLE 1,CIERRE DE CONEXIONES Y DEPURACIÓN
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

#Manejo de errores de la operación realizada en Scala

failed_task = spark.conf.get("failed_task")

if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")

# COMMAND ----------

# DBTITLE 1,NOTIFICACIÓN DE FINALIZADO
notification_raised(webhook_url, 0, "DONE", source, input_parameters)
