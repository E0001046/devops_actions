# Databricks notebook source
# MAGIC %md
# MAGIC ### Version Mejorada del __init__

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC %reload_ext autoreload
# MAGIC import sys
# MAGIC import configparser
# MAGIC import logging
# MAGIC import inspect
# MAGIC from pyspark.sql.functions import lit
# MAGIC from pyspark.sql import SparkSession
# MAGIC from pyspark.sql.functions import col
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
# MAGIC debug = False
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
# MAGIC         "table_002",
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
# MAGIC     process_name = 'root'
# MAGIC     input_values()
# MAGIC     if global_params["status"] == '0':
# MAGIC         logger.error("Revisar mensajes en los logs")
# MAGIC         raise Exception("Error en los valores de entrada, revisar logs")
# MAGIC
# MAGIC     webhook_url, channel, failed_task = conf_init_values(
# MAGIC         config_files['general'], process_name, "IDC"
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

conf_values = [ (fields['step_id'], '\n'.join(fields['value'])) for line, value in file_config_sql.items() if line == 'steps' for fields in value ]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aqui creamos `temp_vigentes_02_{sr_id_archivo}` (debug = True)

# COMMAND ----------

# DBTITLE 1,testing / dev / support
if debug:
    from pyspark.sql import SparkSession, Row
    from pyspark.sql.types import StructType, StructField, StringType, DecimalType
    from decimal import Decimal
    import random

    # Define the schema
    schema = StructType([
        StructField("FTN_NSS_CURP", StringType(), True),
        StructField("FTN_NUM_CTA_INVDUAL", DecimalType(10, 0), True),
        StructField("FCC_VALOR_IND", StringType(), True),
        StructField("FCN_ID_IND_CTA_INDV", DecimalType(38, 0), True),
        StructField("FTC_NOMBRE_BUC", StringType(), True),
        StructField("FTC_AP_PATERNO_BUC", StringType(), True),
        StructField("FTC_AP_MATERNO_BUC", StringType(), True),
        StructField("FTC_RFC_BUC", StringType(), True),
        StructField("FTC_FOLIO", StringType(), True),
        StructField("FTN_ID_ARCHIVO", DecimalType(10, 0), True),
        StructField("FTN_NSS", StringType(), True),
        StructField("FTC_CURP", StringType(), True),
        StructField("FTC_RFC", StringType(), True),
        StructField("FTC_NOMBRE_CTE", StringType(), True),
        StructField("FTC_CLAVE_ENT_RECEP", StringType(), True),
        StructField("FCN_ID_TIPO_SUBCTA", DecimalType(10, 0), True)
    ])

    # Create a list of rows (Row) with the data
    data = [
        Row("CURP001", Decimal(1234567890), "10", Decimal("10000000000000000000000000000000000000"), "Nombre1", "ApellidoP1", "ApellidoM1", "RFC1", "Folio1", Decimal(1234567890), "NSS1", "CURP1", "RFC1", "Cliente1", "Clave1", Decimal(1234567890)),
        Row("CURP012", Decimal(1234567891), "12", Decimal("10000000000000000000000000000000000001"), "Nombre2", "ApellidoP2", "ApellidoM2", "RFC2", "Folio2", Decimal(1234567891), "NSS2", "CURP2", "RFC2", "Cliente2", "Clave2", Decimal(1234567891)),
        Row("CURP003", Decimal(1234567892), "22", Decimal("10000000000000000000000000000000000002"), "Nombre3", "ApellidoP3", "ApellidoM3", "RFC3", "Folio3", Decimal(1234567892), "NSS3", "CURP3", "RFC3", "Cliente3", "Clave3", Decimal(1234567892)),
        Row("CURP004", Decimal(1234567893), "13", Decimal("10000000000000000000000000000000000003"), "Nombre4", "ApellidoP4", "ApellidoM4", "RFC4", "Folio4", Decimal(1234567893), "NSS4", "CURP4", "RFC4", "Cliente4", "Clave4", Decimal(1234567893)),
        Row("CURP005", Decimal(1234567894), "14", Decimal("10000000000000000000000000000000000004"), "Nombre5", "ApellidoP5", "ApellidoM5", "RFC5", "Folio5", Decimal(1234567894), "NSS5", "CURP5", "RFC5", "Cliente5", "Clave5", Decimal(1234567894)),
        Row("CURP006", Decimal(1234567895), "15", Decimal("10000000000000000000000000000000000005"), "Nombre6", "ApellidoP6", "ApellidoM6", "RFC6", "Folio6", Decimal(1234567895), "NSS6", "CURP6", "RFC6", "Cliente6", "Clave6", Decimal(1234567895)),
        Row("CURP007", Decimal(1234567896), "16", Decimal("10000000000000000000000000000000000006"), "Nombre7", "ApellidoP7", "ApellidoM7", "RFC7", "Folio7", Decimal(1234567896), "NSS7", "CURP7", "RFC7", "Cliente7", "Clave7", Decimal(1234567896)),
        Row("CURP008", Decimal(1234567897), "17", Decimal("10000000000000000000000000000000000007"), "Nombre8", "ApellidoP8", "ApellidoM8", "RFC8", "Folio8", Decimal(1234567897), "NSS8", "CURP8", "RFC8", "Cliente8", "Clave8", Decimal(1234567897)),
        Row("CURP009", Decimal(1234567898), "28", Decimal("10000000000000000000000000000000000008"), "Nombre9", "ApellidoP9", "ApellidoM9", "RFC9", "Folio9", Decimal(1234567898), "NSS9", "CURP9", "RFC9", "Cliente9", "Clave9", Decimal(1234567898)),
        Row("CURP010", Decimal(1234567899), "59", Decimal("10000000000000000000000000000000000009"), "Nombre10", "ApellidoP10", "ApellidoM10", "RFC10", "Folio10", Decimal(1234567899), "NSS10", "CURP10", "RFC10", "Cliente10", "Clave10", Decimal(1234567899))
    ]

    # Create a DataFrame from the list of data
    data_df = spark.createDataFrame(data, schema)

    # Create or replace the global temporary view with the data
    data_df.createOrReplaceGlobalTempView(f"temp_vigentes_02_{global_params['sr_id_archivo']}")

    # Get the list of all views in the global_temp database
    vistas_globales_df = spark.catalog.listTables("global_temp")

    # Display the views
    for vista in vistas_globales_df:
        print(f"Vista: {vista.name}, Tipo: {vista.tableType}")

    vigentes_02_DF = spark.sql(
        f"SELECT * FROM global_temp.temp_vigentes_02_{global_params['sr_id_archivo']}"
    )

    display(vigentes_02_DF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aqui creamos `temp_novigentes_03_{sr_id_archivo}` (debug = True)

# COMMAND ----------

# DBTITLE 1,testing / dev / support
if debug:
    from pyspark.sql import SparkSession, Row
    from pyspark.sql.types import StructType, StructField, StringType, DecimalType
    from decimal import Decimal
    import random

    # Definir el esquema
    schema = StructType([
        StructField("FTN_NSS_CURP", StringType(), True),
        StructField("FTN_NUM_CTA_INVDUAL", DecimalType(10, 0), True),
        StructField("FTN_ID_ARCHIVO", DecimalType(10, 0), True),
        StructField("FCC_VALOR_IND", StringType(), True),
        StructField("FTN_NSS", StringType(), True),
        StructField("FTC_CURP", StringType(), True),
        StructField("FTC_RFC", StringType(), True),
        StructField("FTC_NOMBRE_CTE", StringType(), True),
        StructField("FTC_FOLIO", StringType(), True),
        StructField("FTC_NOMBRE_BUC", StringType(), True),
        StructField("FTC_AP_PATERNO_BUC", StringType(), True),
        StructField("FTC_AP_MATERNO_BUC", StringType(), True),
        StructField("FTC_RFC_BUC", StringType(), True),
        StructField("FTC_CLAVE_ENT_RECEP", StringType(), True),
        StructField("FTN_DETALLE", DecimalType(10, 0), True),
        StructField("FCN_ID_TIPO_SUBCTA", DecimalType(10, 0), True)
    ])

    # Generar valores al azar para FTN_DETALLE, asegurando que algunos sean 0, 372 o 212
    detalles = [131, 372, 212, 132, 372, 212, 133, 372, 212, 134, 372, 212]

    # Crear una lista de filas (Row) con los datos
    data = [
        Row("CURP001", Decimal(1234567890), Decimal(29288), "11", "NSS1", "CURP11", "RFC1", "Cliente1", "Folio1", "Nombre1", "ApellidoP1", "ApellidoM1", "RFC1", "190", Decimal(detalles[0]), Decimal(1234567890)),
        Row("CURP012", Decimal(1234567891), Decimal(29288), "19", "NSS2", "CURP12", "RFC2", "Cliente2", "Folio2", "Nombre2", "ApellidoP2", "ApellidoM2", "RFC2", "200", Decimal(detalles[1]), Decimal(1234567891)),
        Row("CURP013", Decimal(1234567892), Decimal(29288), "24", "NSS3", "CURP13", "RFC3", "Cliente3", "Folio3", "Nombre3", "ApellidoP3", "ApellidoM3", "RFC3", "210", Decimal(detalles[2]), Decimal(1234567892)),
        Row("CURP004", Decimal(1234567893), Decimal(29288), "96", "NSS4", "CURP14", "RFC4", "Cliente4", "Folio4", "Nombre4", "ApellidoP4", "ApellidoM4", "RFC4", "220", Decimal(detalles[3]), Decimal(1234567893)),
        Row("CURP015", Decimal(1234567894), Decimal(29288), "55", "NSS5", "CURP15", "RFC5", "Cliente5", "Folio5", "Nombre5", "ApellidoP5", "ApellidoM5", "RFC5", "230", Decimal(detalles[4]), Decimal(1234567894)),
        Row("CURP016", Decimal(1234567895), Decimal(29288), "14", "NSS6", "CURP16", "RFC6", "Cliente6", "Folio6", "Nombre6", "ApellidoP6", "ApellidoM6", "RFC6", "240", Decimal(detalles[5]), Decimal(1234567895)),
        Row("CURP007", Decimal(1234567896), Decimal(29288), "80", "NSS7", "CURP17", "RFC7", "Cliente7", "Folio7", "Nombre7", "ApellidoP7", "ApellidoM7", "RFC7", "250", Decimal(detalles[6]), Decimal(1234567896)),
        Row("CURP018", Decimal(1234567897), Decimal(29288), "69", "NSS8", "CURP18", "RFC8", "Cliente8", "Folio8", "Nombre8", "ApellidoP8", "ApellidoM8", "RFC8", "260", Decimal(detalles[7]), Decimal(1234567897)),
        Row("CURP019", Decimal(1234567898), Decimal(29288), "28", "NSS9", "CURP19", "RFC9", "Cliente9", "Folio9", "Nombre9", "ApellidoP9", "ApellidoM9", "RFC9", "270", Decimal(detalles[8]), Decimal(1234567898)),
        Row("CURP010", Decimal(1234567899), Decimal(29288), "99", "NSS10", "CURP20", "RFC10", "Cliente10", "Folio10", "Nombre10", "ApellidoP10", "ApellidoM10", "RFC10", "280", Decimal(detalles[9]), Decimal(1234567899))
    ]

    # Crear un DataFrame a partir de la lista de datos
    data_df = spark.createDataFrame(data, schema)

    # Create or replace the global temporary view with the data
    data_df.createOrReplaceGlobalTempView(f"temp_novigentes_03_{global_params['sr_id_archivo']}")

    # Get the list of all views in the global_temp database
    vistas_globales_df = spark.catalog.listTables("global_temp")

    # Display the views
    for vista in vistas_globales_df:
        print(f"Vista: {vista.name}, Tipo: {vista.tableType}")

    novigentes_03_DF = spark.sql(
        f"SELECT * FROM global_temp.temp_novigentes_03_{global_params['sr_id_archivo']}"
    )

    display(novigentes_03_DF)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Leemos las vistas temporales que dan inicio al flujo
# MAGIC ### Pasos: 
# MAGIC - Consultar global_temp.temp_novigentes_03_{sr_id_archivo} y almacenarla en `no_vigentes_03_DF​`
# MAGIC - Consultar global_temp.temp_vigentes_02_{sr_id_archivo}​ y almacenarla en `vigentes_02_DF`

# COMMAND ----------

vigentes_02_DF = spark.sql(
    f"SELECT * FROM global_temp.temp_vigentes_02_{global_params['sr_id_archivo']}"
)

# Metemos vigentes_02_DF en cache
vigentes_02_DF.cache()

if debug:
    display(vigentes_02_DF)

# COMMAND ----------

novigentes_03_DF = spark.sql(
    f"SELECT * FROM global_temp.temp_novigentes_03_{global_params['sr_id_archivo']}"
)

# Metemos novigentes_03_DF en cache
novigentes_03_DF.cache()

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

# Crear el DataFrame df_vigentes_derecha donde FTN_DETALLE no sea 372 ni 212
df_vigentes_derecha = novigentes_03_DF[(novigentes_03_DF['FTN_DETALLE'] != 372) & (novigentes_03_DF['FTN_DETALLE'] != 212)]

# Crear el DataFrame df_no_vigentes_abajo donde FTN_DETALLE sea 372 o 212
df_no_vigentes_abajo = novigentes_03_DF[(novigentes_03_DF['FTN_DETALLE'] == 372) | (novigentes_03_DF['FTN_DETALLE'] == 212)]

# Metemos los DataFrames en cache
df_vigentes_derecha.cache()
df_no_vigentes_abajo.cache()

if debug:
    display(df_vigentes_derecha)
    display(df_no_vigentes_abajo)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Aqui hacemos el union con el DF `vigentes_02_DF`

# COMMAND ----------

if debug:
    vigentes_02_DF.printSchema()
    df_vigentes_derecha.printSchema()

# Realizar la unión de los DataFrames
df_vigentes_identificados = df_vigentes_derecha.unionByName(vigentes_02_DF, allowMissingColumns=True)

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

if debug:
    # Mostrar el DataFrame unido
    display(df_identificados_totales)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Con este stage agrupamos y sumamos por FTN_NSS_CURP,  para saber si existen más de 1 registro (significaría que la cuenta está duplicada) Esto se guarda en el campo CUENTASVIG​

# COMMAND ----------

from pyspark.sql import functions as F

# Agrupar por 'FTN_NSS_CURP' y contar las ocurrencias (número de cuentas) para CUENTASVIG
df_vigentes_sum = df_identificados_totales.groupBy("FTN_NSS_CURP").agg(
    F.count("FTN_NSS_CURP").alias("CUENTASVIG")
)

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

# MAGIC %md 
# MAGIC ### Se realiza un Unión a los registros y se genera el archivo TEMP_Encontrados_06​

# COMMAND ----------

# Seleccionar las columnas específicas
df_filtered = df_final.select(
    "FTN_NSS_CURP",
    "FTC_IDENTIFICADOS",
    "FTN_NUM_CTA_INVDUAL",
    "FTN_ID_DIAGNOSTICO",
    "FTN_ID_SUBP_NO_CONV",
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
    "FTN_VIGENCIA",
    "MARC_DUP",
    "FCN_ID_TIPO_SUBCTA"
)

# Crear una Global Temporary View
view_name = f"temp_encontrados_06_{global_params['sr_id_archivo']}".strip()
df_filtered.createOrReplaceGlobalTempView(view_name)

if debug:
    # Verificar que la vista se ha creado correctamente
    display(spark.sql(f"SELECT * FROM global_temp.{view_name}"))
