# Databricks notebook source
import sys
import inspect
import configparser
import json
import logging
import uuid

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)
from pyspark.sql.functions import length, lit
from pyspark.sql.functions import col, lit, current_timestamp


debug = False


def input_values():
    """Retrieve input values from widgets.
    Returns:
        tuple: A tuple containing the input values and a status flag.
    """
    try:
        dbutils.widgets.text("sr_proceso", "")
        dbutils.widgets.text("sr_subproceso", "")
        dbutils.widgets.text("sr_subetapa", "")
        dbutils.widgets.text("sr_origen_arc", "")
        dbutils.widgets.text("sr_dt_org_arc", "")
        dbutils.widgets.text("sr_folio", "")
        dbutils.widgets.text("sr_id_archivo", "")
        dbutils.widgets.text("sr_tipo_layout", "")
        dbutils.widgets.text("sr_instancia_proceso", "")
        dbutils.widgets.text("sr_usuario", "")
        dbutils.widgets.text("sr_etapa", "")
        dbutils.widgets.text("sr_id_snapshot", "")
        dbutils.widgets.text("sr_paso", "")

        sr_proceso = dbutils.widgets.get("sr_proceso")
        sr_subproceso = dbutils.widgets.get("sr_subproceso")
        sr_subetapa = dbutils.widgets.get("sr_subetapa")
        sr_origen_arc = dbutils.widgets.get("sr_origen_arc")
        sr_dt_org_arc = dbutils.widgets.get("sr_dt_org_arc")
        sr_folio = dbutils.widgets.get("sr_folio")
        sr_id_archivo = dbutils.widgets.get("sr_id_archivo")
        sr_tipo_layout = dbutils.widgets.get("sr_tipo_layout")
        sr_instancia_proceso = dbutils.widgets.get("sr_instancia_proceso")
        sr_usuario = dbutils.widgets.get("sr_usuario")
        sr_etapa = dbutils.widgets.get("sr_etapa")
        sr_id_snapshot = dbutils.widgets.get("sr_id_snapshot")
        sr_paso = dbutils.widgets.get("sr_paso")

        if any(
            len(str(value).strip()) == 0
            for value in [
                sr_proceso,
                sr_subproceso,
                sr_subetapa,
                sr_origen_arc,
                sr_dt_org_arc,
                sr_folio,
                sr_id_archivo,
                sr_tipo_layout,
                sr_instancia_proceso,
                sr_usuario,
                sr_etapa,
                sr_id_snapshot,
                sr_paso,
            ]
        ):
            logger.error("Function: %s", inspect.stack()[0][3])
            logger.error("Some of the input values are empty or null")
            return "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"
    except Exception as e:
        logger.error("Function: %s", inspect.stack()[0][3])
        logger.error("An error was raised: %s", str(e))
        return "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"
    return (
        sr_proceso,
        sr_subproceso,
        sr_subetapa,
        sr_origen_arc,
        sr_dt_org_arc,
        sr_folio,
        sr_id_archivo,
        sr_tipo_layout,
        sr_instancia_proceso,
        sr_usuario,
        sr_etapa,
        sr_id_snapshot,
        sr_paso,
        "1",
    )


def conf_process_values(config_file, process_name):
    """Retrieve process configuration values from a config file.
    Args:
        config_file (str): Path to the configuration file.
        process_name (str): Name of the process.
    Returns:
        tuple: A tuple containing the configuration values and a status flag.
    """
    try:
        config = configparser.ConfigParser()
        config.read(config_file)
        # Subprocess configurations
        #
        sql_conf_file = config.get(process_name, "sql_conf_file")
        conn_schema_001 = config.get(process_name, "conn_schema_001")
        conn_schema_002 = config.get(process_name, "conn_schema_002")
        table_002 = config.get(process_name, "table_002")
        table_010 = config.get(process_name, "table_010")
        table_011 = config.get(process_name, "table_011")
        table_012 = config.get(process_name, "table_012")

    except Exception as e:
        logger.error("Function: %s", inspect.stack()[0][3])
        logger.error("An error was raised: " + str(e))
        return "0", "0", "0", "0", "0", "0", "0"
    return (
        sql_conf_file,
        conn_schema_001,
        conn_schema_002,
        table_002,
        table_010,
        table_011,
        table_012,
        "1",
    )


if __name__ == "__main__":
    logging.basicConfig()
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    notebook_name = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )
    message = "NB Error: " + notebook_name
    source = "ETL"
    root_repo = "/Workspace/Shared/MITAFO"

    try:
        sys.path.append(root_repo + "/" + "CGRLS_0010/Notebooks")
        from NB_GRLS_DML_FUNCTIONS import *
        from NB_GRLS_SIMPLE_FUNCTIONS import *
    except Exception as e:
        logger.error("Error at the beginning of the process")
        logger.error("An error was raised: " + str(e))

    config_file = root_repo + "/" + "CGRLS_0010/Conf/CF_GRLS_PROCESS.py.properties"
    config_conn_file = root_repo + "/" + "CGRLS_0010/Conf/CF_GRLS_CONN.py.properties"
    config_process_file = (
        root_repo
        + "/"
        + "ANCIN_0030/Jobs/01_IDENTIFICACION_DE_CLIENTE/01_DISPERSIONES/Conf/CF_PART_PROC.py.properties"
    )

    (
        sr_proceso,
        sr_subproceso,
        sr_subetapa,
        sr_origen_arc,
        sr_dt_org_arc,
        sr_folio,
        sr_id_archivo,
        sr_tipo_layout,
        sr_instancia_proceso,
        sr_usuario,
        sr_etapa,
        sr_id_snapshot,
        sr_paso,
        failed_task,
    ) = input_values()
    if failed_task == "0":
        logger.error("Please review log messages")
        # notification_raised(webhook_url, -1, message, source, input_parameters)
        raise Exception("An error occurred, check out log messages")

    input_parameters = dbutils.widgets.getAll().items()

    process_name = "root"
    webhook_url, channel, failed_task = conf_init_values(
        config_file, process_name, "IDC"
    )
    if failed_task == "0":
        logger.error("Please review log messages")
        notification_raised(webhook_url, -1, message, source, input_parameters)
        raise Exception("Process ends")

    process_name = "root"
    (
        sql_conf_file,
        conn_schema_001,
        conn_schema_002,
        table_002,
        table_010,
        table_011,
        table_012,
        failed_task,
    ) = conf_process_values(config_process_file, process_name)
    if failed_task == "0":
        logger.error("Please review log messages")
        notification_raised(webhook_url, -1, message, source, input_parameters)
        raise Exception("Process ends")

    conn_name_ora = "jdbc_oracle"
    (
        conn_options,
        conn_aditional_options,
        conn_user,
        conn_key,
        conn_url,
        failed_task,
    ) = conf_conn_values(config_conn_file, conn_name_ora)
    if failed_task == "0":
        logger.error("Please review log messages")
        notification_raised(webhook_url, -1, message, source, input_parameters)
        raise Exception("Process ends")

    sql_conf_file = (
        root_repo
        + "/"
        + "/ANCIN_0030/Jobs/01_IDENTIFICACION_DE_CLIENTE/01_DISPERSIONES/JSON/"
        + sql_conf_file
    )

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
table_name = f"{conn_schema_001}.{table_002}"

params = [
    table_name,
    sr_folio,
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

# Particionamos el df original en 2 partes
df = df.repartition(100)

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

# Crear el DataFrame df_verdes, excluyendo los registros donde FTN_ESTATUS_DIAG sea "1" o NULL
df_verdes = df.filter((col("FTN_ESTATUS_DIAG") != "1") | col("FTN_ESTATUS_DIAG").isNull())

# Reparticionar los dataframes
df_rojos = df_rojos.repartition(100)
df_verdes = df_verdes.repartition(100)

# Metemos los dataframes en cache
df_rojos.cache()
df_verdes.cache()

if debug:
    display(df_rojo)
    display(df_verde)

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

df_rojos.cache()
df_verdes.cache()

df_rojos = df_rojos.withColumn("TOTALES", lit(total_rojos))
df_verdes = df_verdes.withColumn("TOTAL_ERROR", lit(total_verdes))

if debug:
    df_rojos.printSchema()
    df_verdes.printSchema()
    display(df_rojos)
    display(df_verdes)

# COMMAND ----------

# MAGIC %md
# MAGIC Se realiza el join por el campo Folio obtener los campos TOTALES y TOTAL_ERROR​
# MAGIC El resultado de esto es solo una fila​

# COMMAND ----------

from pyspark.sql.functions import col

# Eliminar la columna FTN_ESTATUS_DIAG de df_rojos y df_verdes
df_rojos = df_rojos.drop("FTN_ESTATUS_DIAG")
df_verdes = df_verdes.drop("FTN_ESTATUS_DIAG")

# Realizar el join entre df_rojos y df_verdes por el campo FTC_FOLIO
df_joined = df_rojos.join(df_verdes, on="FTC_FOLIO", how="left")

# Repartir df_joined
df_joined = df_joined.repartition(100)

# Reemplazar NULL por 0 en las columnas TOTALES y TOTAL_ERROR
df_joined = df_joined.fillna(0, subset=["TOTALES", "TOTAL_ERROR"])

# Meter df_joined en cache
df_joined.cache()

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
# MAGIC FLN_REG_CUMPLIERON = TOTALES - TOTAL_ERROR
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    from_utc_timestamp,
    to_timestamp,
)

# Crear el nuevo DataFrame aplicando las reglas
df_new = df_joined.select(
    col("FTC_FOLIO").alias("FTC_FOLIO"),
    lit(24).alias("FTC_ID_SUBETAPA"),
    col("TOTALES").alias("FLN_TOTAL_REGISTROS"),
    col("TOTAL_ERROR").alias("FLN_REG_NO_CUMPLIERON"),
    lit(f"{conn_user}").alias("FLC_USU_REG"),
    lit(None).alias("FLC_DETALLE"),  # Esto asigna un valor nulo
    lit("93").alias("FLC_VALIDACION"),
    to_timestamp(from_utc_timestamp(current_timestamp(), "GMT-6")).alias("FLD_FEC_REG"),
    lit(0).alias("FLN_TOTAL_ERRORES"),
    (col("TOTALES") - col("TOTAL_ERROR")).alias("FLN_REG_CUMPLIERON"),
)

# Meter el nuevo DataFrame en cache
df_new.cache()

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
# MAGIC ### Borro el registro en la tabla en OCI `CIERREN_ETL.TLSISGRAL_ETL_VAL_CIFRAS_CTRL`

# COMMAND ----------

table_name_01 = f"{conn_schema_001}.{table_011}" # CIERREN_ETL.TLSISGRAL_ETL_VAL_CIFRAS_CTRL

statement = f"""
DELETE FROM {table_name_01}
WHERE FTC_FOLIO = {sr_folio}
    AND FTC_ID_SUBETAPA = {sr_subetapa}
"""
if debug:
    statement

# COMMAND ----------

spark.conf.set("conn_url", str(conn_url))
spark.conf.set("conn_user", str(conn_user))
spark.conf.set("conn_key", str(conn_key))
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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Borro el registro en la tabla en OCI `CIERREN.TLSISGRAL_VAL_CIFRAS_CTRL`

# COMMAND ----------

table_name_02 = f"{conn_schema_002}.{table_012}" # CIERREN.TLSISGRAL_VAL_CIFRAS_CTRL

statement = f"""
DELETE FROM {table_name_02}
WHERE FTC_FOLIO = {sr_folio}
    AND FTC_ID_SUBETAPA = {sr_subetapa}
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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hacemos los inserts a las tablas.

# COMMAND ----------

target_table = f"{conn_schema_001}.{table_011}"
mode = "APPEND"
failed_task = write_into_table(
    conn_name_ora,
    df_new,
    mode,
    target_table,
    conn_options,
    conn_aditional_options,
    conn_user,
    conn_key,
)

if failed_task == "0":
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")

# COMMAND ----------

target_table = f"{conn_schema_002}.{table_012}"
mode = "APPEND"
failed_task = write_into_table(
    conn_name_ora,
    df_new,
    mode,
    target_table,
    conn_options,
    conn_aditional_options,
    conn_user,
    conn_key,
)

if failed_task == "0":
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")

# COMMAND ----------

from pyspark.sql import DataFrame

# Clear cache
spark.catalog.clearCache()

# Unpersist and delete all DataFrames
for df_name in list(globals()):
    if isinstance(globals()[df_name], DataFrame):
        globals()[df_name].unpersist()
        del globals()[df_name]
