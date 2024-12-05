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
        sql_conf_file = config.get(process_name, "sql_conf_file")
        conn_schema_001 = config.get(process_name, "conn_schema_001")
        table_002 = config.get(process_name, "table_002")
        # Unity
        debug = config.get(process_name, "debug")
        debug = debug.lower() == "true"
        catalog_name = config.get(process_name, "catalog_name")
        schema_name = config.get(process_name, "schema_name")

    except Exception as e:
        logger.error("Function: %s", inspect.stack()[0][3])
        logger.error("An error was raised: " + str(e))
        return (
            "0",
            "0",
            "0",
            "0",
            "0",
            "0",
            "0",
        )
    return (
        sql_conf_file,
        conn_schema_001,
        table_002,
        # unity
        debug,
        catalog_name,
        schema_name,
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
        + "ANCIN_0030/Jobs/01_IDENTIFICACION_DE_CLIENTE/01_DISPERSIONES/Conf/CF_PART_PROC_UNITY.py.properties"
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
        table_002,
        # unity
        debug,
        catalog_name,
        schema_name,
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
        scope,
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

conf_values = [ (fields['step_id'], '\n'.join(fields['value'])) for line, value in file_config_sql.items() if line == 'steps' for fields in value ]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Leemos las vistas temporales que dan inicio al flujo
# MAGIC ### Pasos: 
# MAGIC - Consultar global_temp.temp_novigentes_03_{sr_id_archivo} y almacenarla en `no_vigentes_03_DF​`
# MAGIC - Consultar global_temp.temp_vigentes_02_{sr_id_archivo}​ y almacenarla en `vigentes_02_DF`

# COMMAND ----------

vigentes_02_DF = spark.sql(
    f"SELECT * FROM {catalog_name}.{schema_name}.temp_vigentes_{sr_id_archivo}"
)

# Metemos vigentes_02_DF en cache
vigentes_02_DF.cache()
colums = [
    "FTN_NSS_CURP",
    "FTN_NUM_CTA_INVDUAL",
    "FTN_ID_ARCHIVO",
    "FTN_NSS",
    "FTC_CURP",
    "FTC_RFC",
    "FTC_NOMBRE_CTE",
    "FTC_FOLIO",
    "FTC_NOMBRE_BUC",
    "FTC_AP_PATERNO_BUC",
    "FTC_AP_MATERNO_BUC",
    "FTC_RFC_BUC",
    "FTC_CLAVE_ENT_RECEP",
    "FCC_VALOR_IND",
    "FCN_ID_TIPO_SUBCTA",
]
vigentes_02_DF = vigentes_02_DF.select(colums)
vigentes_02_DF = vigentes_02_DF.withColumn(
    "FTN_NUM_CTA_INVDUAL",
    vigentes_02_DF["FTN_NUM_CTA_INVDUAL"].cast("decimal")
)
if debug:
    display(vigentes_02_DF)

# COMMAND ----------

novigentes_03_DF = spark.sql(
    f"SELECT * FROM {catalog_name}.{schema_name}.temp_novigentes_{sr_id_archivo}"
)

# Metemos novigentes_03_DF en cache
novigentes_03_DF.cache()
colums = [
    "FTN_NSS_CURP",
    "FTN_NUM_CTA_INVDUAL",
    "FTN_ID_ARCHIVO",
    "FTN_NSS",
    "FTC_CURP",
    "FTC_RFC",
    "FTC_NOMBRE_CTE",
    "FTC_FOLIO",
    "FTC_NOMBRE_BUC",
    "FTC_AP_PATERNO_BUC",
    "FTC_AP_MATERNO_BUC",
    "FTC_RFC_BUC",
    "FTC_CLAVE_ENT_RECEP",
    "FTN_DETALLE",
    "FCC_VALOR_IND",
    "FCN_ID_TIPO_SUBCTA",
]
novigentes_03_DF = novigentes_03_DF.select(colums)
novigentes_03_DF = novigentes_03_DF.withColumn(
    "FTN_NUM_CTA_INVDUAL",
    novigentes_03_DF["FTN_NUM_CTA_INVDUAL"].cast("decimal")
)
if debug:
    display(novigentes_03_DF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aplicamos un order by al df novigentes_03_DF por FTN_NSS_CURP de manera ascendente

# COMMAND ----------

novigentes_03_DF = novigentes_03_DF.orderBy("FTN_NSS_CURP", ascending=True)
if debug:
    display(novigentes_03_DF)

# COMMAND ----------

# MAGIC %md
# MAGIC Del archivo de cuentas NO VIGENTES (`temp_novigentes_03`) se evalúa que la cuenta no haya sido puesta como NO VIGENTE por el subproceso:​
# MAGIC
# MAGIC - 372 DESCERTIFICACION DE CUENTAS ​
# MAGIC - 212 CANCELACION DE CUENTAS POR SALDO CERO​
# MAGIC
# MAGIC Notas:
# MAGIC - En caso de que otro proceso diferente a estos los haya puesto con vigencia = 0 el flujo continúa hacia la derecha para juntarlos con las cuentas VIGENTES del archivo temp_vigentes_02 (`df_vigentes_identificados`) ​
# MAGIC - En caso de que los procesos anteriores los haya puesto con vigencia = 0, el registro continúa hacía la parte inferior para generarle los campos FTC_IDENTIFICADOS, FTN_ID_DIAGNOSTICO, FTN_VIGENCIA (`df_no_vigentes_abajo`)

# COMMAND ----------

# MAGIC %md
# MAGIC ### NOTA:
# MAGIC - Debemos evaluar si es posible es escenario donde FTN_DETALLE venga con valores nulos, y si esi los debemos considerar???

# COMMAND ----------

from pyspark.sql.functions import col, isnull

# Crear el DataFrame df_vigentes_derecha donde FTN_DETALLE sea distinto de 372, 212 o sea NULL
df_vigentes_derecha = novigentes_03_DF.filter(
    (col("FTN_DETALLE") != 372) & 
    (col("FTN_DETALLE") != 212) | 
    (isnull(col("FTN_DETALLE")))
)

# Crear el DataFrame df_no_vigentes_abajo con todos los registros que NO estén en df_vigentes_derecha
df_no_vigentes_abajo = novigentes_03_DF.join(df_vigentes_derecha, ["FTN_DETALLE"], "left_anti")

# Metemos los DataFrames en cache
df_vigentes_derecha.cache()
df_no_vigentes_abajo.cache()

if debug:
    display(df_vigentes_derecha)
    display(df_no_vigentes_abajo)

# COMMAND ----------

df_no_vigentes_abajo = df_no_vigentes_abajo.withColumnRenamed("FTN_DETALLE", "FTN_ID_SUBP_NO_CONV")
if debug:
    display(df_no_vigentes_abajo)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Aqui hacemos el union con el DF `vigentes_02_DF`

# COMMAND ----------

if debug:
    vigentes_02_DF.printSchema()
    df_vigentes_derecha.printSchema()

# Realizar la unión de los DataFrames
df_vigentes_identificados = df_vigentes_derecha.unionByName(
    vigentes_02_DF, allowMissingColumns=True
)

df_vigentes_identificados = df_vigentes_identificados.orderBy(
    ["FTN_NSS_CURP", "FTN_NUM_CTA_INVDUAL"],
    ascending=True,
)

# Metemos el DataFrame en cache
df_vigentes_identificados.cache()

if debug:
    # Mostrar el DataFrame unido
    display(df_vigentes_identificados)

# COMMAND ----------

# MAGIC %md
# MAGIC # La siguiente celda es solo para pruebas (debug = True)

# COMMAND ----------

# DBTITLE 1,testing / dev / support
if debug:
    # Identificar los duplicados basados en las columnas FTN_NSS_CURP y FTN_NUM_CTA_INVDUAL
    duplicados = df_vigentes_identificados.groupBy(
        'FTN_NSS_CURP', 
        'FTN_NUM_CTA_INVDUAL'
    ).count().filter('count > 1')

    # Mostrar los duplicados
    display(duplicados)


# COMMAND ----------

# MAGIC %md
# MAGIC Quitamos los duplicados al df `df_vigentes_identificados` por los campos `FTN_NSS_CURP` y  `FTN_NUM_CTA_INVDUAL` para dejar la info a nivel cuenta individual 

# COMMAND ----------

# Eliminar duplicados en el DataFrame original basado en las columnas especificadas
df_vigentes_identificados_unique = df_vigentes_identificados.drop_duplicates(subset=['FTN_NSS_CURP', 'FTN_NUM_CTA_INVDUAL'])

# Metemos el DataFrame en cache
df_vigentes_identificados_unique.cache()

if debug:
    # Mostrar las primeras filas del DataFrame resultante
    display(df_vigentes_identificados_unique)


# COMMAND ----------

# MAGIC %md
# MAGIC ### - Con `df_vigentes_identificados_unique`
# MAGIC - Mapeamos estas cuentas como como identificadas (FTC_IDENTIFICADOS = 1)
# MAGIC   - Reglas:
# MAGIC   ```text
# MAGIC     FTN_NSS_CURP = FTN_NSS_CURP
# MAGIC     FTC_IDENTIFICADOS = 1
# MAGIC     FTN_NUM_CTA_INVDUAL = FTN_NUM_CTA_INVDUAL
# MAGIC     FTN_ID_DIAGNOSTICO = SetNull()
# MAGIC     FTC_NOMBRE_BUC = FTC_NOMBRE_BUC
# MAGIC     FTC_AP_PATERNO_BUC = FTC_AP_PATERNO_BUC
# MAGIC     FTC_AP_MATERNO_BUC= FTC_AP_MATERNO_BUC
# MAGIC     FTC_RFC_BUC = FTC_RFC_BUC
# MAGIC     FTC_FOLIO =	FTC_FOLIO
# MAGIC     FTN_ID_ARCHIVO = FTN_ID_ARCHIVO
# MAGIC     FTN_NSS =	FTN_NSS
# MAGIC     FTC_CURP = FTC_CURP
# MAGIC     FTC_RFC = FTC_RFC
# MAGIC     FTC_NOMBRE_CTE = FTC_NOMBRE_CTE
# MAGIC     FTC_CLAVE_ENT_RECEP = FTC_CLAVE_ENT_RECEP
# MAGIC     FTN_ID_SUBP_NO_CONV = SetNull()
# MAGIC     FTN_VIGENCIA = Left(Trim(FCC_VALOR_IND),1)
# MAGIC     FCN_ID_TIPO_SUBCTA = FCN_ID_TIPO_SUBCTA
# MAGIC     ```
# MAGIC ### - Con `df_no_vigentes_abajo`
# MAGIC - Aplicamos estas reglas:
# MAGIC   ```text
# MAGIC     FTC_IDENTIFICADOS = 0​
# MAGIC     FTN_ID_DIAGNOSTICO =  254 (CLIENTE NO VIGENTE)​
# MAGIC     FTN_VIGENCIA = 0  
# MAGIC   ```
# MAGIC
# MAGIC - Posteriormente unir con el UNION al set de datos `df_vigentes_identificados_unique` con `df_no_vigentes_abajo`

# COMMAND ----------

from pyspark.sql.functions import trim, col, lit, substring

# Modificar el DataFrame existente para añadir o modificar las columnas según las reglas especificadas
df_vigentes_identificados_unique = df_vigentes_identificados_unique.withColumn("FTC_IDENTIFICADOS", lit(1)) \
    .withColumn("FTN_ID_DIAGNOSTICO", lit(None)) \
    .withColumn("FTC_NOMBRE_BUC", col("FTC_NOMBRE_BUC")) \
    .withColumn("FTC_AP_PATERNO_BUC", col("FTC_AP_PATERNO_BUC")) \
    .withColumn("FTC_AP_MATERNO_BUC", col("FTC_AP_MATERNO_BUC")) \
    .withColumn("FTC_RFC_BUC", col("FTC_RFC_BUC")) \
    .withColumn("FTC_FOLIO", col("FTC_FOLIO")) \
    .withColumn("FTN_ID_ARCHIVO", col("FTN_ID_ARCHIVO")) \
    .withColumn("FTN_NSS", col("FTN_NSS")) \
    .withColumn("FTC_CURP", col("FTC_CURP")) \
    .withColumn("FTC_RFC", col("FTC_RFC")) \
    .withColumn("FTC_NOMBRE_CTE", col("FTC_NOMBRE_CTE")) \
    .withColumn("FTC_CLAVE_ENT_RECEP", col("FTC_CLAVE_ENT_RECEP")) \
    .withColumn("FTN_ID_SUBP_NO_CONV", lit(None)) \
    .withColumn("FTN_VIGENCIA", substring(trim(col("FCC_VALOR_IND")), 1, 1)) \
    .withColumn("FCN_ID_TIPO_SUBCTA", col("FCN_ID_TIPO_SUBCTA"))

# Metemos el DataFrame en cache
df_vigentes_identificados_unique.cache()
columnas = [
    "FTN_NSS_CURP",
    "FTC_IDENTIFICADOS",
    "FTN_NUM_CTA_INVDUAL",
    "FTN_ID_DIAGNOSTICO",
    "FTC_NOMBRE_BUC",
    "FTC_AP_PATERNO_BUC",
    "FTC_AP_MATERNO_BUC",
    "FTC_RFC_BUC",
    "FTC_FOLIO",
    "FTN_ID_ARCHIVO",
    "FTN_NSS",
    "FTC_CURP",
    "FTC_RFC",
    "FTC_NOMBRE_CTE",
    "FTC_CLAVE_ENT_RECEP",
    "FTN_ID_SUBP_NO_CONV",
    "FTN_VIGENCIA",
    "FCN_ID_TIPO_SUBCTA",
]
df_vigentes_identificados_unique = df_vigentes_identificados_unique.select(columnas)

if debug:
    # Mostrar las primeras filas del DataFrame modificado
    display(df_vigentes_identificados_unique)


# COMMAND ----------

# Modificar el DataFrame existente para agregar las nuevas columnas
df_no_vigentes_abajo = df_no_vigentes_abajo.withColumn("FTC_IDENTIFICADOS", lit(0)) \
                                           .withColumn("FTN_ID_DIAGNOSTICO", lit("254")) \
                                           .withColumn("FTN_VIGENCIA", lit(0))

# Metemos el DataFrame en cache
df_no_vigentes_abajo.cache()

columnas = [
    "FTN_NSS_CURP",
    "FTC_IDENTIFICADOS",
    "FTN_NUM_CTA_INVDUAL",
    "FTN_ID_DIAGNOSTICO",
    "FTC_NOMBRE_BUC",
    "FTC_AP_PATERNO_BUC",
    "FTC_AP_MATERNO_BUC",
    "FTC_RFC_BUC",
    "FTC_FOLIO",
    "FTN_ID_ARCHIVO",
    "FTN_NSS",
    "FTC_CURP",
    "FTC_RFC",
    "FTC_NOMBRE_CTE",
    "FTC_CLAVE_ENT_RECEP",
    "FTN_ID_SUBP_NO_CONV",
    "FTN_VIGENCIA",
    "FCN_ID_TIPO_SUBCTA",
]
df_no_vigentes_abajo = df_no_vigentes_abajo.select(columnas)
if debug:
    # Mostrar las primeras filas del DataFrame modificado
    display(df_no_vigentes_abajo)

# COMMAND ----------

# MAGIC %md
# MAGIC # Aqui hacemos el UNION

# COMMAND ----------

df_vigentes_identificados_unique.printSchema()
df_no_vigentes_abajo.printSchema()

# Realizar la unión de los DataFrames
df_identificados_totales = df_vigentes_identificados_unique.unionByName(df_no_vigentes_abajo, allowMissingColumns=True)

# Metemos el DataFrame en cache
df_identificados_totales.cache()

df_identificados_totales.orderBy("FTN_NSS_CURP", ascending=True)

if debug:
    # Mostrar el DataFrame unido
    display(df_identificados_totales)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Con este stage agrupamos y sumamos por FTN_NSS_CURP,  para saber si existen más de 1 registro (significaría que la cuenta está duplicada) Esto se guarda en el campo CUENTASVIG​ (La sumatorias se hace por el campo FTN_VIGENCIA
# MAGIC )

# COMMAND ----------

from pyspark.sql.functions import sum as _sum

# Agrupar por FTN_NSS_CURP y sumar los valores de FTN_VIGENCIA
df_vigentes_sum = df_identificados_totales.groupBy("FTN_NSS_CURP").agg(_sum("FTN_VIGENCIA").alias("CUENTASVIG"))


# Metemos el DataFrame en cache
df_vigentes_sum.cache()

if debug:
    # Mostrar las primeras filas del DataFrame resultante
    display(df_vigentes_sum)


# COMMAND ----------

# MAGIC %md
# MAGIC ### A la información que hemos procesado en la parte superior, se le realiza un left join por FTN_NSS_CURP para obtener el campo CUENTASVIG​

# COMMAND ----------

df_joined = df_identificados_totales.join(
    df_vigentes_sum, 
    on="FTN_NSS_CURP", 
    how="left"
)

# Metemos el DataFrame en cache
df_joined.cache()

if debug:
    display(df_joined)

# COMMAND ----------

# MAGIC %md
# MAGIC En este Filter se realizan las siguientes validaciones​
# MAGIC
# MAGIC - CUENTASVIG > 1 AND FTN_VIGENCIA = 1 (hay más de una cuenta vigente, ambas con indicador de vigencia = 1)​
# MAGIC   - Le genera el campo MARC_DUP y le asigna el valor ‘D’​
# MAGIC
# MAGIC - CUENTASVIG < 2 (Hay una sola cuenta vigente)​
# MAGIC   - Le genera el campo MARC_DUP y le asigna el valor ‘’ (vacío)

# COMMAND ----------

from pyspark.sql.functions import col, when

# Aplicar las validaciones y generar el campo MARC_DUP
df_final = df_joined.withColumn(
    "MARC_DUP",
    when((col("CUENTASVIG") > 1) & (col("FTN_VIGENCIA") == 1), "D")
    .when(col("CUENTASVIG") < 2, "")
    .otherwise("")
)

# Metemos el DataFrame en cache
df_final.cache()

if debug:
    display(df_final)

# COMMAND ----------

df_final = df_final.drop("CUENTASVIG")
view_name = f"temp_encontrados_06_{sr_id_archivo}".strip()
df_final.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog_name}.{schema_name}.{view_name}"
)

#df_filtered.createOrReplaceGlobalTempView(view_name)

if debug:
    # Verificar que la vista se ha creado correctamente
    display(spark.sql(f"SELECT * FROM {catalog_name}.{schema_name}.{view_name}"))


# COMMAND ----------

# Liberar los DataFrames cacheados
vigentes_02_DF.unpersist()
novigentes_03_DF.unpersist()
df_vigentes_derecha.unpersist()
df_no_vigentes_abajo.unpersist()
df_vigentes_identificados.unpersist()
df_vigentes_identificados_unique.unpersist()
df_identificados_totales.unpersist()
df_vigentes_sum.unpersist()
df_joined.unpersist()
df_final.unpersist()

# Eliminar las referencias a los DataFrames
del vigentes_02_DF
del novigentes_03_DF
del df_vigentes_derecha
del df_no_vigentes_abajo
del df_vigentes_identificados
del df_vigentes_identificados_unique
del df_identificados_totales
del df_vigentes_sum
del df_joined
del df_final

# Forzar la recolección de basura para liberar memoria
import gc
gc.collect()
