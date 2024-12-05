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
# MAGIC prod = True  # Para setear paths
# MAGIC
# MAGIC # Variables globales
# MAGIC
# MAGIC root_repo = "/Workspace/Shared/MITAFO"
# MAGIC config_files = {
# MAGIC     "general": f"{root_repo}/CGRLS_0010/Conf/CF_GRLS_PROCESS.py.properties",
# MAGIC     "connection": f"{root_repo}/CGRLS_0010/Conf/CF_GRLS_CONN.py.properties",
# MAGIC     "process": f"{root_repo}/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/DISPERSIONES/Conf/CF_PART_PROC.py.properties"
# MAGIC     if prod
# MAGIC     else "/Workspace/Repos/mronboye@emeal.nttdata.com/QueryConfigLab.ide/"
# MAGIC     "MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/"
# MAGIC     "DISPERSIONES/Conf/"
# MAGIC     "CF_PART_PROC.py.properties",
# MAGIC }
# MAGIC
# MAGIC notebook_name = (
# MAGIC     dbutils.notebook.entry_point.getDbutils()
# MAGIC     .notebook()
# MAGIC     .getContext()
# MAGIC     .notebookPath()
# MAGIC     .get()
# MAGIC )
# MAGIC message = "NB Error: " + notebook_name
# MAGIC source = "ETL"
# MAGIC
# MAGIC process_name = "root"
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
# MAGIC global_confs = {}  # Diccionario para almacenar las keys globales
# MAGIC
# MAGIC
# MAGIC def input_values() -> dict:
# MAGIC     """Obtiene los valores de los widgets de entrada y los almacena en un diccionario global."""
# MAGIC
# MAGIC     widget_defaults = {
# MAGIC         "SR_FOLIO_REL": "",
# MAGIC         "SR_PROCESO": "",
# MAGIC         "SR_FECHA_LIQ": "",
# MAGIC         "SR_TIPO_MOV": "",
# MAGIC         "SR_REPROCESO": "",
# MAGIC         "SR_SUBPROCESO": "",
# MAGIC         "SR_USUARIO": "",
# MAGIC         "SR_INSTANCIA_PROCESO": "",
# MAGIC         "SR_ORIGEN_ARC": "",
# MAGIC         "SR_ID_SNAPSHOT": "",
# MAGIC         "SR_FECHA_ACC": "",
# MAGIC         "SR_FOLIO": "",
# MAGIC         "SR_SUBETAPA": "",
# MAGIC         "SR_FACTOR": "",
# MAGIC         "SR_ETAPA": "",
# MAGIC     }
# MAGIC
# MAGIC     # Crear los widgets en minúsculas
# MAGIC     for key, default_value in widget_defaults.items():
# MAGIC         dbutils.widgets.text(key.lower(), default_value)
# MAGIC
# MAGIC     # Actualizar el diccionario global en mayúsculas para el resto del notebook
# MAGIC     global_params.update(
# MAGIC         {key.upper(): dbutils.widgets.get(key.lower()).strip() for key in widget_defaults}
# MAGIC     )
# MAGIC
# MAGIC     if any(not value for value in global_params.values()):
# MAGIC         logger.error("Valores de entrada vacíos o nulos")
# MAGIC         global_params["status"] = "0"
# MAGIC     else:
# MAGIC         global_params["status"] = "1"
# MAGIC
# MAGIC     return global_params
# MAGIC
# MAGIC
# MAGIC def conf_process_values(arg_config_file: str, arg_process_name: str) -> tuple:
# MAGIC     """Obtiene los valores de configuración del proceso y los almacena en un diccionario global."""
# MAGIC     keys = [
# MAGIC         "sql_conf_file",
# MAGIC         "debug",
# MAGIC         "conn_schema_001",
# MAGIC         "conn_schema_002",
# MAGIC         "table_001",
# MAGIC         "table_002",
# MAGIC         "catalog_name",
# MAGIC         "schema_name",
# MAGIC     ]
# MAGIC
# MAGIC     try:
# MAGIC         config = configparser.ConfigParser()
# MAGIC         config.read(arg_config_file)
# MAGIC         result = {key: config.get(arg_process_name, key) for key in keys}
# MAGIC         result["status"] = "1"
# MAGIC         # Almacenar los valores en el diccionario global
# MAGIC         global_confs.update(result)
# MAGIC     except (ValueError, IOError) as error:
# MAGIC         logger.error("Error en la función %s: %s", inspect.stack()[0][3], error)
# MAGIC         result = {key: "0" for key in keys}
# MAGIC         result["status"] = "0"
# MAGIC         # Almacenar los valores en el diccionario global
# MAGIC         global_confs.update(result)
# MAGIC
# MAGIC     return tuple(result.values())
# MAGIC
# MAGIC # Configuración del manejador global de excepciones
# MAGIC def global_exception_handler(exc_type, exc_value, exc_traceback):
# MAGIC     if issubclass(exc_type, KeyboardInterrupt):
# MAGIC         # Permitir que KeyboardInterrupt se maneje normalmente
# MAGIC         sys.__excepthook__(exc_type, exc_value, exc_traceback)
# MAGIC         return
# MAGIC
# MAGIC     message = f"Uncaught exception: {exc_value}"
# MAGIC     source = "ETL"
# MAGIC     input_parameters = dbutils.widgets.getAll().items()
# MAGIC     # Registro del error y notificación
# MAGIC     logger.error("Please review log messages")
# MAGIC     notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC     raise Exception("An error raised")
# MAGIC
# MAGIC # Asigna el manejador de excepciones al hook global de sys
# MAGIC sys.excepthook = global_exception_handler
# MAGIC
# MAGIC
# MAGIC if __name__ == "__main__":
# MAGIC     # Inicialización de variables
# MAGIC     input_parameters = dbutils.widgets.getAll().items()
# MAGIC
# MAGIC     webhook_url, channel, failed_task = conf_init_values(
# MAGIC         config_files["general"], process_name, "TEMP_PROCESS"
# MAGIC     )
# MAGIC
# MAGIC     input_values()
# MAGIC     if global_params["status"] == "0":
# MAGIC         logger.error("Revisar mensajes en los logs")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Error en los valores de entrada, revisar logs")
# MAGIC
# MAGIC     if failed_task == "0":
# MAGIC         logger.error("Please review log MESSAGEs")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     process_name = "root"
# MAGIC     conf_values = conf_process_values(config_files["process"], process_name)
# MAGIC     if conf_values[-1] == "0":
# MAGIC         logger.error("Revisar mensajes en los logs")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Error en la configuración del proceso, revisar logs")
# MAGIC
# MAGIC     conn_name_ora = "jdbc_oracle"
# MAGIC     (
# MAGIC         conn_options,
# MAGIC         conn_additional_options,
# MAGIC         conn_user,
# MAGIC         conn_key,
# MAGIC         conn_url,
# MAGIC         scope,
# MAGIC         failed_task,
# MAGIC     ) = conf_conn_values(config_files["connection"], conn_name_ora)
# MAGIC     if failed_task == "0":
# MAGIC         logger.error("Revisar mensajes en los logs")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Error en la configuración de la conexión, revisar logs")
# MAGIC
# MAGIC     if prod:
# MAGIC         sql_conf_file = f"{root_repo}/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/DISPERSIONES/JSON/{conf_values[0]}"
# MAGIC     else:
# MAGIC         sql_conf_file = f"/Workspace/Repos/mronboye@emeal.nttdata.com/QueryConfigLab.ide/MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/DISPERSIONES/JSON/{conf_values[0]}"
# MAGIC     # Seteamos el valor de debug
# MAGIC     debug = conf_values[1]
# MAGIC     debug = debug.lower() == 'true'

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
# MAGIC ### Creamos el `DF_02_DISPERSIONES` a partir de la vista temporal `TEMP_DISPERSION_MOV_02`

# COMMAND ----------

table_name = f"TEMP_DISPERSION_MOV_{global_params['SR_FOLIO']}"

# Create the DataFrame DF_02_DISPERSIONES from the temporary view TEMP_DISPERSION_MOV_02
DF_02_DISPERSIONES = spark.sql(f"SELECT * FROM {global_confs['catalog_name']}.{global_confs['schema_name']}.{table_name}")

# Inserto DF_02_DISPERSIONES al cache
DF_02_DISPERSIONES.cache()

if debug:
    display(DF_02_DISPERSIONES)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Renombramos campos

# COMMAND ----------

# Rename the column FFN_ID_CONCEPTO_MOV to FCN_ID_CONCEPTO_MOV in main_df
DF_02_DISPERSIONES = DF_02_DISPERSIONES.withColumnRenamed("FTC_USU_CRE", "FCC_USU_CRE")
DF_02_DISPERSIONES = DF_02_DISPERSIONES.withColumnRenamed("FTC_USU_ACT", "FCC_USU_ACT")
DF_02_DISPERSIONES = DF_02_DISPERSIONES.withColumnRenamed("FFN_ID_CONCEPTO_MOV", "FCN_ID_CONCEPTO_MOV")
DF_02_DISPERSIONES = DF_02_DISPERSIONES.withColumnRenamed("FTC_TABLA_NCI_MOV", "FCC_TABLA_NCI_MOV")



if debug:
    display(DF_02_DISPERSIONES)
    DF_02_DISPERSIONES.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aplicamos estas reglas, solo a los campos que cumplan con la condicion:
# MAGIC ```sql
# MAGIC IF p_SR_ETAPA = 1 OR p_SR_ETAPA = 3
# MAGIC ```
# MAGIC ### Reglas
# MAGIC ```text
# MAGIC FTN_ID_MOV=CONTADOR2
# MAGIC FCN_ID_TIPO_SUBCTA=FCN_ID_TIPO_SUBCTA
# MAGIC FTC_FOLIO=FTC_FOLIO
# MAGIC FTC_FOLIO_REL=FTC_FOLIO_REL
# MAGIC FCN_ID_SIEFORE=FCN_ID_SIEFORE
# MAGIC FTF_MONTO_PESOS=FTF_MONTO_PESOS
# MAGIC FTF_MONTO_ACCIONES=FTF_MONTO_ACCIONES
# MAGIC FCN_ID_VALOR_ACCION=FCN_ID_VALOR_ACCION
# MAGIC FTN_NUM_CTA_INVDUAL=FTN_NUM_CTA_INVDUAL
# MAGIC FTN_ID_SEMAFORO_MOV=FTN_ID_SEMAFORO_MOV
# MAGIC FTD_FEH_LIQUIDACION=FTD_FEH_LIQUIDACION
# MAGIC FTD_ID_ESTATUS_MOV=FTD_ID_ESTATUS_MOV
# MAGIC FTN_ID_MARCA=FTN_ID_MARCA
# MAGIC FCN_ID_TIPO_MOV=FCN_ID_TIPO_MOV
# MAGIC FCD_FEH_CRE=Current Timestamp()
# MAGIC FCC_USU_CRE=FCC_USU_CRE
# MAGIC FCD_FEH_ACT=Current Timestamp()
# MAGIC FCC_USU_ACT=FCC_USU_ACT
# MAGIC FCN_ID_CONCEPTO_MOV=FCN_ID_CONCEPTO_MOV
# MAGIC FCC_TABLA_NCI_MOV=FCC_TABLA_NCI_MOV
# MAGIC FNN_ID_REFERENCIA=FNN_ID_REFERENCIA
# MAGIC FTN_ID_ERROR_VAL=FTN_ID_ERROR_VAL
# MAGIC FTN_MOV_GENERADO=FTN_MOV_GENERADO
# MAGIC FTN_REG_ACREDITADO=FTN_REG_ACREDITADO
# MAGIC ```

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    from_utc_timestamp,
    to_timestamp,
)

# Parámetros
p_SR_TIPO_MOV = global_params["SR_TIPO_MOV"]
p_SR_FACTOR = global_params["SR_FACTOR"]
p_SR_ETAPA = global_params["SR_ETAPA"]

# Verificar si el valor de p_SR_ETAPA es 1 o 3
if int(p_SR_ETAPA) in [1, 3]:

    DF_ETL_MOVS_01 = DF_02_DISPERSIONES

    # Inserto DF_ETL_MOVS_01 al cache
    DF_ETL_MOVS_01.cache()

    # Aplicar las reglas al nuevo DataFrame DF_ETL_MOVS_01
    DF_ETL_MOVS_01 = (
        DF_ETL_MOVS_01.withColumn("FTN_ID_MOV", F.col("CONTADOR2"))
        .withColumn("FCN_ID_TIPO_SUBCTA", F.col("FCN_ID_TIPO_SUBCTA"))
        .withColumn("FTC_FOLIO", F.col("FTC_FOLIO"))
        .withColumn("FTC_FOLIO_REL", F.col("FTC_FOLIO_REL"))
        .withColumn("FCN_ID_SIEFORE", F.col("FCN_ID_SIEFORE"))
        .withColumn("FTF_MONTO_PESOS", F.col("FTF_MONTO_PESOS"))
        .withColumn("FTF_MONTO_ACCIONES", F.col("FTF_MONTO_ACCIONES"))
        .withColumn("FCN_ID_VALOR_ACCION", F.col("FCN_ID_VALOR_ACCION"))
        .withColumn("FTN_NUM_CTA_INVDUAL", F.col("FTN_NUM_CTA_INVDUAL"))
        .withColumn("FTN_ID_SEMAFORO_MOV", F.col("FTN_ID_SEMAFORO_MOV"))
        .withColumn("FTD_FEH_LIQUIDACION", F.col("FTD_FEH_LIQUIDACION"))
        .withColumn("FTD_ID_ESTATUS_MOV", F.col("FTD_ID_ESTATUS_MOV"))
        .withColumn("FTN_ID_MARCA", F.col("FTN_ID_MARCA"))
        .withColumn("FCN_ID_TIPO_MOV", F.col("FCN_ID_TIPO_MOV"))
        .withColumn(
            "FCD_FEH_CRE", F.to_timestamp(from_utc_timestamp(current_timestamp(), "GMT-6"))
        )
        .withColumn("FCC_USU_CRE", F.col("FCC_USU_CRE"))
        .withColumn(
            "FCD_FEH_ACT", F.to_timestamp(from_utc_timestamp(current_timestamp(), "GMT-6"))
        )
        .withColumn("FCC_USU_ACT", F.col("FCC_USU_ACT"))
        .withColumn("FCN_ID_CONCEPTO_MOV", F.col("FCN_ID_CONCEPTO_MOV"))
        .withColumn("FCC_TABLA_NCI_MOV", F.col("FCC_TABLA_NCI_MOV"))
        .withColumn("FNN_ID_REFERENCIA", F.col("FNN_ID_REFERENCIA"))
        .withColumn("FTN_ID_ERROR_VAL", F.col("FTN_ID_ERROR_VAL"))
        .withColumn("FTN_MOV_GENERADO", F.col("FTN_MOV_GENERADO"))
        .withColumn("FTN_REG_ACREDITADO", F.col("FTN_REG_ACREDITADO"))
    )

    if debug:
        # Mostrar el resultado
        display(DF_ETL_MOVS_01)
        DF_ETL_MOVS_01.count()

    # unpersist DF_02_DISPERSIONES
    DF_02_DISPERSIONES.unpersist()
    del DF_02_DISPERSIONES

else:
    print(f"p_SR_ETAPA ({p_SR_ETAPA}) no cumple con la condición para aplicar las reglas.")


# COMMAND ----------

# MAGIC %md
# MAGIC ### El `DF_ETL_MOVS_01` se inserta en OCI, a la tabla `CIERREN_ETL.TTSISGRAL_ETL_MOVIMIENTOS`

# COMMAND ----------

# Lista de columnas que deseas seleccionar
if int(p_SR_ETAPA) in [1, 3]:
    columns_to_select = [
        "FTN_ID_MOV",
        "FCN_ID_TIPO_SUBCTA",
        "FTC_FOLIO",
        "FTC_FOLIO_REL",
        "FCN_ID_SIEFORE",
        "FTF_MONTO_PESOS",
        "FTF_MONTO_ACCIONES",
        "FCN_ID_VALOR_ACCION",
        "FTN_NUM_CTA_INVDUAL",
        "FTN_ID_SEMAFORO_MOV",
        "FTD_FEH_LIQUIDACION",
        "FTD_ID_ESTATUS_MOV",
        "FTN_ID_MARCA",
        "FCN_ID_TIPO_MOV",
        "FCD_FEH_CRE",
        "FCC_USU_CRE",
        "FCD_FEH_ACT",
        "FCC_USU_ACT",
        "FCN_ID_CONCEPTO_MOV",
        "FCC_TABLA_NCI_MOV",
        "FNN_ID_REFERENCIA",
        "FTN_ID_ERROR_VAL",
        "FTN_MOV_GENERADO",
        "FTN_REG_ACREDITADO"
    ]


# COMMAND ----------

if int(p_SR_ETAPA) in [1, 3]:
    DF_ETL_MOVS_01 = DF_ETL_MOVS_01.select(*columns_to_select)

    if debug:
        display(DF_ETL_MOVS_01)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pero primero antes de insertar, debemos hacer un delete a la misma tabla

# COMMAND ----------

if int(p_SR_ETAPA) in [1, 3]:
    target_table = f"{global_confs['conn_schema_001']}.{global_confs['table_002']}"
    statement = f"""
    DELETE /*+ PARALLEL(CIERREN_ETL.TTSISGRAL_ETL_MOVIMIENTOS, 8) */ FROM  {target_table}
    WHERE FTC_FOLIO = '{global_params['SR_FOLIO']}' 
    AND (CASE WHEN {global_params['SR_TIPO_MOV']} = 2 THEN '1'
                    ELSE FTC_FOLIO_REL
            END) = DECODE({global_params['SR_TIPO_MOV']},2,'1','{global_params['SR_FOLIO_REL']}')
    """
    if debug:
        print(statement)

# COMMAND ----------

if int(p_SR_ETAPA) in [1, 3]:
    spark.conf.set("conn_url", str(conn_url))
    spark.conf.set("conn_user", str(conn_user))
    spark.conf.set("conn_key", str(conn_key))
    spark.conf.set("statement", str(statement))
    spark.conf.set("scope", str(scope))
    spark.conf.set("is_empty", str(0))
else:
    spark.conf.set("is_empty", str(1))

# COMMAND ----------

# MAGIC %scala
# MAGIC import java.util.Properties
# MAGIC import java.sql.DriverManager
# MAGIC val is_empty = spark.conf.get("is_empty")
# MAGIC if (is_empty != "1"){
# MAGIC     val conn_user = spark.conf.get("conn_user")
# MAGIC     val conn_key = spark.conf.get("conn_key")
# MAGIC     val conn_url = spark.conf.get("conn_url")
# MAGIC     val conn_scope = spark.conf.get("scope")
# MAGIC     val driverClass = "oracle.jdbc.driver.OracleDriver"
# MAGIC
# MAGIC     Class.forName(driverClass)
# MAGIC
# MAGIC     val connectionProperties = new Properties()
# MAGIC     var connection: java.sql.Connection = null
# MAGIC     try {
# MAGIC         connectionProperties.setProperty("user", dbutils.secrets.get(scope=conn_scope, key=conn_user))
# MAGIC         connectionProperties.setProperty("password", dbutils.secrets.get(scope=conn_scope, key=conn_key))
# MAGIC         connectionProperties.setProperty("v$session.osuser", dbutils.secrets.get(scope=conn_scope, key=conn_user))
# MAGIC
# MAGIC         connection = DriverManager.getConnection(conn_url, connectionProperties)
# MAGIC         val stmt = connection.createStatement()
# MAGIC         val sql = spark.conf.get("statement")
# MAGIC
# MAGIC         stmt.execute(sql)
# MAGIC         spark.conf.set("failed_task", "1")
# MAGIC     }
# MAGIC     catch {
# MAGIC         case _: Throwable => spark.conf.set("failed_task", "0")
# MAGIC     } finally {
# MAGIC         if (connection != null) connection.close()
# MAGIC     }
# MAGIC }

# COMMAND ----------

#Manejo de errores de la operación realizada en Scala
if int(p_SR_ETAPA) in [1, 3]:
    failed_task = spark.conf.get("failed_task")

    if failed_task == '0':
        logger.error("Please review log messages")
        notification_raised(webhook_url, -1, message, source, input_parameters)
        raise Exception("An error raised")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora si hacemos el insert

# COMMAND ----------

target_table = f"{global_confs['conn_schema_001']}.{global_confs['table_002']}"
#dbutils.secrets.get(scope=conn_scope, key=conn_user)
jdbc_properties = {
    "user": dbutils.secrets.get(scope=scope, key=conn_user),
    "password": dbutils.secrets.get(scope=scope, key=conn_key),
    "driver": "oracle.jdbc.driver.OracleDriver",
    "fetchSize": "20000",  # Ajustado para optimizar las lecturas desde Oracle
    "batchsize": "5000",  # Tamaño de batch aumentado para reducir la cantidad de transacciones
    "numPartitions": "1",  # Ajustado para realizar inserciones secuenciales (sin concurrencia)
    "queryTimeout": "0",  # Tiempo de espera indefinido para evitar timeouts prematuros
    "connectRetryCount": "10",  # Número de intentos de reconexión en caso de fallos de conexión
    "connectRetryInterval": "10"  # Intervalo en segundos entre intentos de reconexión
}

# Aumentar tiempo de espera para bloqueos a 120 segundos
lock_timeout = "120000"  # 120 segundos

max_retries = 3  # Número máximo de reintentos
retry_count = 0
batch_size = 100000  # Número de registros por cada batch

# Verificar el conteo del DataFrame antes de hacer el randomSplit
df_count = DF_ETL_MOVS_01.count()

if df_count > 0:
    # Dividimos el DataFrame en bloques más pequeños usando randomSplit si el conteo es mayor a cero
    if df_count > batch_size:
        fractions = [batch_size / df_count] * (df_count // batch_size)
    else:
        fractions = [1.0]
    sub_dataframes = DF_ETL_MOVS_01.randomSplit(fractions)
else:
    # Si el conteo es cero, dejamos fractions igual al DataFrame original
    sub_dataframes = [DF_ETL_MOVS_01]

# Bucle para manejar la inserción en bloques más pequeños y los reintentos en caso de deadlocks
while retry_count < max_retries:
    try:
        # Iterar sobre cada sub-DataFrame (bloque) generado por randomSplit o el DataFrame original si el conteo es cero
        for batch_df in sub_dataframes:
            batch_df.write \
                .format("jdbc") \
                .option("url", conn_url) \
                .option("dbtable", target_table) \
                .option("user", jdbc_properties["user"]) \
                .option("password", jdbc_properties["password"]) \
                .option("driver", jdbc_properties["driver"]) \
                .option("fetchSize", jdbc_properties["fetchSize"]) \
                .option("batchsize", jdbc_properties["batchsize"]) \
                .option("numPartitions", jdbc_properties["numPartitions"]) \
                .option("queryTimeout", jdbc_properties["queryTimeout"]) \
                .option("connectRetryCount", jdbc_properties["connectRetryCount"]) \
                .option("connectRetryInterval", jdbc_properties["connectRetryInterval"]) \
                .option("isolationLevel", "READ_COMMITTED") \
                .option("oracle.jdbc.lockTimeout", lock_timeout) \
                .mode("append") \
                .save()
        
        break  # Sale del bucle si tiene éxito
    except Exception as e:
        # Convertimos la excepción a cadena para revisar su contenido
        error_message = str(e)
        
        if "ORA-00060" in error_message:  # Deadlock detected
            retry_count += 1
            logger.warning(f"ORA-00060 deadlock detected. Retry {retry_count}/{max_retries}")
            if retry_count == max_retries:
                raise Exception("Max retries reached. Could not complete the operation due to deadlock.")
        elif "ORA-08177" in error_message:  # Serialization failure
            retry_count += 1
            logger.warning(f"ORA-08177 serialization failure detected. Retry {retry_count}/{max_retries}")
            if retry_count == max_retries:
                raise Exception("Max retries reached. Could not complete the operation due to serialization failure.")
        else:
            # Si no es un deadlock o error de serialización, lanzar error inesperado
            logger.error(f"Unexpected error: {error_message}")
            notification_raised(webhook_url, -1, message, source, input_parameters)
            raise e  # Re-levanta la excepción si no es ORA-08177 o ORA-00060

# COMMAND ----------

#target_table_aux = "CIERREN_DATAUX.TTSISGRAL_ETL_MOVIMIENTOS_TMP"
# if int(p_SR_ETAPA) in [1, 3]:
#     mode = "APPEND"
#     failed_task = write_into_table(
#         conn_name_ora,
#         DF_ETL_MOVS_01,
#         mode,
#         target_table,
#         conn_options,
#         conn_additional_options,
#         conn_user,
#         conn_key,
#     )

#     if failed_task == "0":
#         logger.error("Please review log messages")
#         notification_raised(webhook_url, -1, message, source, input_parameters)
#         raise Exception("An error raised")

# COMMAND ----------

from pyspark.sql import DataFrame

# Clear cache
spark.catalog.clearCache()

# Unpersist and delete all DataFrames
for df_name in list(globals()):
    if isinstance(globals()[df_name], DataFrame):
        globals()[df_name].unpersist()
        del globals()[df_name]

# COMMAND ----------

# Liberar la caché del DataFrame si se usó cache
DF_ETL_MOVS_01.unpersist()

# Eliminar DataFrames para liberar memoria
del DF_ETL_MOVS_01

# Recolector de basura para liberar recursos inmediatamente
import gc
gc.collect()
