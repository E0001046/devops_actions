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
# MAGIC # Variables globales
# MAGIC root_repo = '/Workspace/Shared/MITAFO'
# MAGIC config_files = {
# MAGIC     "general": f"{root_repo}/CGRLS_0010/Conf/CF_GRLS_PROCESS.py.properties",
# MAGIC     "connection": f"{root_repo}/CGRLS_0010/Conf/CF_GRLS_CONN.py.properties",
# MAGIC     "process": f"{root_repo}/ANCIN_0030/Jobs/01_IDENTIFICACION_DE_CLIENTE/01_DISPERSIONES/Conf/CF_PART_PROC.py.properties",
# MAGIC }
# MAGIC notebook_name = (
# MAGIC         dbutils.notebook.entry_point.getDbutils()
# MAGIC         .notebook()
# MAGIC         .getContext()
# MAGIC         .notebookPath()
# MAGIC         .get()
# MAGIC     )
# MAGIC message = "NB Error: " + notebook_name
# MAGIC source = "ETL"
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
# MAGIC conf_global = {}  # Diccionario para almacenar las keys globales
# MAGIC debug = True
# MAGIC
# MAGIC def input_values() -> dict:
# MAGIC     """Obtiene los valores de los widgets de entrada y los almacena en un diccionario global."""
# MAGIC     widget_defaults = {
# MAGIC         "sr_proceso": "7",
# MAGIC         "sr_subproceso": "8",
# MAGIC         "sr_subetapa": "1",
# MAGIC         "sr_origen_arc": "1",
# MAGIC         "sr_dt_org_arc": "1",
# MAGIC         "sr_folio": "202311211301042007",
# MAGIC         "sr_id_archivo": "29288",
# MAGIC         "sr_tipo_layout": "1",
# MAGIC     }
# MAGIC     
# MAGIC     for key, default_value in widget_defaults.items():
# MAGIC         dbutils.widgets.text(key, default_value)
# MAGIC     
# MAGIC     global_params.update({key: dbutils.widgets.get(key).strip() for key in widget_defaults})
# MAGIC     
# MAGIC     if any(not value for value in global_params.values()):
# MAGIC         logger.error("Valores de entrada vacíos o nulos")
# MAGIC         global_params["status"] = "0"
# MAGIC     else:
# MAGIC         global_params["status"] = "1"
# MAGIC     
# MAGIC     return global_params
# MAGIC
# MAGIC def conf_process_values(arg_config_file: str, arg_process_name: str) -> tuple:
# MAGIC     """Obtiene los valores de configuración del proceso y los almacena en un diccionario global."""
# MAGIC     keys = [
# MAGIC         "sql_conf_file",
# MAGIC         "conn_schema_001",
# MAGIC         "conn_schema_002",
# MAGIC         "table_002",
# MAGIC         "table_010",
# MAGIC         "table_011",
# MAGIC         "table_012"
# MAGIC     ]
# MAGIC     
# MAGIC     try:
# MAGIC         config = configparser.ConfigParser()
# MAGIC         config.read(arg_config_file)
# MAGIC         result = {key: config.get(arg_process_name, key) for key in keys}
# MAGIC         result["status"] = "1"
# MAGIC         # Almacenar los valores en el diccionario global
# MAGIC         conf_global.update(result)
# MAGIC     except (ValueError, IOError) as error:
# MAGIC         logger.error("Error en la función %s: %s", inspect.stack()[0][3], error)
# MAGIC         result = {key: "0" for key in keys}
# MAGIC         result["status"] = "0"
# MAGIC         # Almacenar los valores en el diccionario global
# MAGIC         conf_global.update(result)
# MAGIC     
# MAGIC     return tuple(result.values())
# MAGIC
# MAGIC if __name__ == "__main__":
# MAGIC     # Inicialización de variables
# MAGIC     input_values()
# MAGIC     if global_params["status"] == '0':
# MAGIC         logger.error("Revisar mensajes en los logs")
# MAGIC         raise Exception("Error en los valores de entrada, revisar logs")
# MAGIC
# MAGIC     input_parameters = dbutils.widgets.getAll().items()
# MAGIC     process_name = 'root'
# MAGIC
# MAGIC     webhook_url, channel, failed_task = conf_init_values(
# MAGIC         config_files['general'], process_name, "TEMP_PROCESS"
# MAGIC     )
# MAGIC
# MAGIC     if failed_task == "0":
# MAGIC         logger.error("Please review log MESSAGEs")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     process_name = 'root'
# MAGIC     conf_values = conf_process_values(config_files["process"], process_name)
# MAGIC     if conf_values[-1] == '0':
# MAGIC         logger.error("Revisar mensajes en los logs")
# MAGIC         raise Exception("Error en la configuración del proceso, revisar logs")
# MAGIC     
# MAGIC     conn_name_ora = 'jdbc_oracle'
# MAGIC     conn_options, conn_additional_options, conn_user, conn_key, conn_url, failed_task = conf_conn_values(config_files["connection"], conn_name_ora)
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Revisar mensajes en los logs")
# MAGIC         raise Exception("Error en la configuración de la conexión, revisar logs")
# MAGIC     
# MAGIC     sql_conf_file = f"{root_repo}/ANCIN_0030/Jobs/01_IDENTIFICACION_DE_CLIENTE/01_DISPERSIONES/JSON/{conf_values[0]}"
# MAGIC

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
# MAGIC ### Hacemos una extraccion de datos de OCI

# COMMAND ----------

query_statement = "020"
table_name = f"{conf_global['conn_schema_001']}.{conf_global['table_002']}"

params = [
    table_name,
    global_params["sr_folio"],
]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == "0":
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, global_params)
    raise Exception("Process ends")

df, failed_task = query_table(
    conn_name_ora, spark, statement, conn_options, conn_user, conn_key
)

if failed_task == "0":
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")

if debug:
    display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC El Filter va a dividir los registros en 2 caminos​
# MAGIC
# MAGIC **Rojo**: Pasarán todos los registros​
# MAGIC
# MAGIC **Verde**: Se irán todos los registros  donde el FTN_ESTATUS_DIAG no sea 1  (cuando no sea un cliente identificado)

# COMMAND ----------

# Renombramos el df original a df_rojo (que contine todos los datos)
df_rojos = df

# Crear el DataFrame df_verde donde FTN_ESTATUS_DIAG no es igual a "1"
df_verdes = df.filter(col("FTN_ESTATUS_DIAG") != "1")

#display(df_rojo)
#display(df_verde)

# COMMAND ----------

total_rojos = df_rojos.count()
total_verdes = df_verdes.count()
if debug:
    print(f"rojos: {total_rojos} - verdes: {total_verdes}")

# COMMAND ----------

# MAGIC %md
# MAGIC El agregator del camino rojo cuenta el total de registros y genera el campo TOTALES​
# MAGIC
# MAGIC El agregator del camino verde cuenta el total de registros con error  y genera el campo TOTAL_ERROR​

# COMMAND ----------

df_rojos = df_rojos.withColumn("TOTALES", lit(total_rojos))
df_verdes = df_verdes.withColumn("TOTAL_ERROR", lit(total_verdes))

if debug:
    df_rojos.printSchema()
    df_verdes.printSchema()

# display(df_rojos)
# display(df_verdes)

# COMMAND ----------

# MAGIC %md
# MAGIC Se realiza el join por el campo Folio obtener los campos TOTALES y TOTAL_ERROR​
# MAGIC El resultado de esto es solo una fila​

# COMMAND ----------

# Eliminar la columna FTN_ESTATUS_DIAG de df_rojos y df_verdes
df_rojos = df_rojos.drop("FTN_ESTATUS_DIAG")
df_verdes = df_verdes.drop("FTN_ESTATUS_DIAG")

# Realizar el join entre df_rojos y df_verdes por el campo FTC_FOLIO
df_joined = df_rojos.join(df_verdes, on="FTC_FOLIO", how="inner")

# Eliminar filas duplicadas basadas en FTC_FOLIO
df_joined = df_joined.dropDuplicates(["FTC_FOLIO"])

# Mostrar el resultado final
if debug:
    df_joined.show()



# COMMAND ----------

# MAGIC %md
# MAGIC ### Genero ahora a partir del DF `df_joined` un nuevo DF con estas reglas:
# MAGIC ```text
# MAGIC FTC_FOLIO = FOLIO
# MAGIC FTC_ID_SUBETAPA = 24
# MAGIC FLN_TOTAL_REGISTROS = TOTALES
# MAGIC FLN_REG_NO_CUMPLIERON = TOTAL_ERROR
# MAGIC FLC_USU_REG = p_CX_CRE_USUARIO
# MAGIC FLC_DETALLE = SetNull()
# MAGIC FLC_VALIDACION = '93'
# MAGIC FLD_FEC_REG = CurrentTimestamp()
# MAGIC FLN_TOTAL_ERRORES = 0
# MAGIC FLN_REG_CUMPLIERON = TOTAL_ERROR
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# Crear el nuevo DataFrame aplicando las reglas
df_new = df_joined.select(
    col("FTC_FOLIO").alias("FTC_FOLIO"),
    lit(24).alias("FTC_ID_SUBETAPA"),
    col("TOTALES").alias("FLN_TOTAL_REGISTROS"),
    col("TOTAL_ERROR").alias("FLN_REG_NO_CUMPLIERON"),
    lit(f"{conn_user}").alias("FLC_USU_REG"),
    lit(None).alias("FLC_DETALLE"),  # Esto asigna un valor nulo
    lit('93').alias("FLC_VALIDACION"),
    current_timestamp().alias("FLD_FEC_REG"),
    lit(0).alias("FLN_TOTAL_ERRORES"),
    col("TOTAL_ERROR").alias("FLN_REG_CUMPLIERON")
)

# Mostrar el nuevo DataFrame
if debug:
    display(df_new)


# COMMAND ----------

# MAGIC %md
# MAGIC Con los datos anteriores realizamos un update then insert en las siguientes 2 tablas por los campos FTC_FOLIO  y FTC_ID_SUBETAPA​
# MAGIC
# MAGIC - `CIERREN_ETL.TLSISGRAL_ETL_VAL_CIFRAS_CTRL​`
# MAGIC - `CIERREN.TLAFOGRAL_VAL_CIFRAS_CONTROL​`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert hacia la tabla AUX en OCI (`CIERREN_ETL.TLAFOGRAL_ETL_VAL_CIFRAS_CONTROL_AUX)`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Antes de insertar a OCI debemos aseguranos que el schema no traiga key con tipo de datos 'void'
# MAGIC Ejemplo: 
# MAGIC FLC_DETALLE: void (nullable = true)

# COMMAND ----------

df_new.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Cambio de tipo de dato VOID
# MAGIC df_new tiene trae el campo FLC_DETALLE: void (nullable = true) 
# MAGIC
# MAGIC Por lo tanto en la celda siguiente se le cambia el tipo de dato.
# MAGIC

# COMMAND ----------

# Asumiendo que estas son las columnas con tipo `void` que deseas modificar
columns_to_modify = {
    "FLC_DETALLE": StringType()
}

# Modificar los tipos de las columnas especificadas
for column, new_type in columns_to_modify.items():
    df_new = df_new.withColumn(column, col(column).cast(new_type))

# Verificar si las columnas siguen siendo nullables
schema = df_new.schema

# Asegurar que las columnas modificadas son nullables
for field in schema.fields:
    if field.name in columns_to_modify:
        print(f"{field.name} is nullable: {field.nullable}")

# Rellenar las columnas con valores predeterminados si son nulos
df_new = df_new.fillna(
    {
        "FLC_DETALLE": "",  # Valor por defecto para cadenas
    }
)

if debug:
    df_new.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Ahora si hacemos el INSERT a la tabla AUX en OCI

# COMMAND ----------

target_table = f"{conf_global['conn_schema_001']}.{conf_global['table_010']}"
mode = "APPEND"
failed_task = write_into_table(
    conn_name_ora,
    df_new,
    mode,
    target_table,
    conn_options,
    conn_additional_options,
    conn_user,
    conn_key,
)

if failed_task == "0":
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge hacia `CIERREN_ETL.TLSISGRAL_ETL_VAL_CIFRAS_CTRL`

# COMMAND ----------

query_statement = "021"
table_name_01 = f"{conf_global['conn_schema_001']}.{conf_global['table_011']}" # CIERREN_ETL.TLSISGRAL_ETL_VAL_CIFRAS_CTRL
table_name_02 = f"{conf_global['conn_schema_001']}.{conf_global['table_010']}" # CIERREN_ETL.TLSISGRAL_ETL_VAL_CIFRAS_CTRL_AUX

params = [
    table_name_01,
    table_name_02,
]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == "0":
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, global_params)
    raise Exception("Process ends")

# COMMAND ----------

statement

# COMMAND ----------

spark.conf.set("conn_url", str(conn_url))
spark.conf.set("conn_user", str(conn_user))
spark.conf.set("conn_key", str(conn_key))
spark.conf.set("statement", str(statement))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enviamos el MERGE con scala

# COMMAND ----------

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

# MAGIC %md
# MAGIC ### Merge hacia `CIERREN_ETL.TLSISGRAL_VAL_CIFRAS_CTRL`

# COMMAND ----------

query_statement = "021"
table_name_01 = f"{conf_global['conn_schema_002']}.{conf_global['table_012']}" # CIERREN_ETL.TLSISGRAL_VAL_CIFRAS_CTRL
table_name_02 = f"{conf_global['conn_schema_001']}.{conf_global['table_010']}" # CIERREN_ETL.TLSISGRAL_ETL_VAL_CIFRAS_CTRL_AUX

params = [
    table_name_01,
    table_name_02,
]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == "0":
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, global_params)
    raise Exception("Process ends")

# COMMAND ----------

if debug:
    statement

# COMMAND ----------

spark.conf.set("statement", str(statement))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enviamos el MERGE con scala

# COMMAND ----------

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

# MAGIC %md
# MAGIC ### Depuramos la tabla AUX

# COMMAND ----------

id_sub_etapa = df_new.select("FTC_ID_SUBETAPA").first()["FTC_ID_SUBETAPA"] # Esta variable la usaremos en el query para depurar la tabla AUX en OCI


# COMMAND ----------

statement = f"""
DELETE FROM {table_name_02}
WHERE FTC_FOLIO = {global_params['sr_folio']}
    AND FTC_ID_SUBETAPA = {id_sub_etapa}
"""
if debug:
    statement

# COMMAND ----------

spark.conf.set("statement", str(statement))

# COMMAND ----------

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
