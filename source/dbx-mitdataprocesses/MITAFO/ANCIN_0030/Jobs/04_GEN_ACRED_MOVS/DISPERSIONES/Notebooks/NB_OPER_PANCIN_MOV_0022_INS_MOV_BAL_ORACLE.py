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
# MAGIC     input_values()
# MAGIC     if global_params["status"] == "0":
# MAGIC         logger.error("Revisar mensajes en los logs")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Error en los valores de entrada, revisar logs")
# MAGIC
# MAGIC     input_parameters = dbutils.widgets.getAll().items()
# MAGIC
# MAGIC     webhook_url, channel, failed_task = conf_init_values(
# MAGIC         config_files["general"], process_name, "TEMP_PROCESS"
# MAGIC     )
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
# MAGIC ### Renombramos los siguientes campos

# COMMAND ----------

# Rename the column FFN_ID_CONCEPTO_MOV to FCN_ID_CONCEPTO_MOV in main_df
DF_02_DISPERSIONES = DF_02_DISPERSIONES.withColumnRenamed("FTC_USU_CRE", "FCC_USU_CRE")
DF_02_DISPERSIONES = DF_02_DISPERSIONES.withColumnRenamed("FTC_USU_ACT", "FCC_USU_ACT")
DF_02_DISPERSIONES = DF_02_DISPERSIONES.withColumnRenamed("FTN_NUM_CTA_INVDUAL", "FTC_NUM_CTA_INVDUAL")
DF_02_DISPERSIONES = DF_02_DISPERSIONES.withColumnRenamed("FFN_ID_CONCEPTO_MOV", "FCN_ID_CONCEPTO_MOV")
DF_02_DISPERSIONES = DF_02_DISPERSIONES.withColumnRenamed("FTC_TABLA_NCI_MOV", "FCC_TABLA_NCI_MOV")


if debug:
    display(DF_02_DISPERSIONES)
    DF_02_DISPERSIONES.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aplicamos estas reglas, solo a los campos que cumplan esta condicion:
# MAGIC ```sql
# MAGIC IF FTN_ID_SEMAFORO_MOV =  190 AND IsNull(FTN_ID_ERROR_VAL) AND p_SR_ETAPA <> 2
# MAGIC ```
# MAGIC ### Reglas:
# MAGIC ```text
# MAGIC CONTADOR = FTN_ID_BAL_MOV
# MAGIC FTC_FOLIO = FTC_FOLIO
# MAGIC FTC_FOLIO_REL = FTC_FOLIO_REL
# MAGIC FTN_DISP_PESOS = 0
# MAGIC FTN_DISP_ACCIONES = IF p_SR_TIPO_MOV = 2 THEN 0 ELSE (FTF_MONTO_ACCIONES *-1) * p_SR_FACTOR
# MAGIC FTN_PDTE_PESOS = IF p_SR_TIPO_MOV = 2 THEN FTF_MONTO_PESOS * p_SR_FACTOR ELSE 0
# MAGIC FTN_PDTE_ACCIONES = IF p_SR_TIPO_MOV = 2 THEN FTF_MONTO_ACCIONES * p_SR_FACTOR ELSE 0
# MAGIC FTN_COMP_PESOS = IF p_SR_TIPO_MOV = 2 THEN 0 ELSE FTF_MONTO_PESOS * p_SR_FACTOR
# MAGIC FTN_COMP_ACCIONES = IF p_SR_TIPO_MOV = 2 THEN 0 ELSE FTF_MONTO_ACCIONES * p_SR_FACTOR
# MAGIC FTN_DIA_PESOS = 0
# MAGIC FTN_DIA_ACCIONES = 0
# MAGIC FTC_NUM_CTA_INVDUAL = FTN_NUM_CTA_INVDUAL
# MAGIC FTN_ORIGEN_APORTACION = IF (FCN_ID_TIPO_SUBCTA=19 OR FCN_ID_TIPO_SUBCTA=21 OR FCN_ID_TIPO_SUBCTA=23) THEN 293 ELSE SetNull
# MAGIC FCN_ID_TIPO_SUBCTA = FCN_ID_TIPO_SUBCTA
# MAGIC FCN_ID_SIEFORE = FCN_ID_SIEFORE
# MAGIC FCN_ID_VALOR_ACCION = FCN_ID_VALOR_ACCION
# MAGIC FTD_FEH_LIQUIDACION = FTD_FEH_LIQUIDACION
# MAGIC FCN_ID_TIPO_MOV = FCN_ID_TIPO_MOV
# MAGIC FCN_ID_CONCEPTO_MOV = FCN_ID_CONCEPTO_MOV
# MAGIC FCC_TABLA_NCI_MOV = FCC_TABLA_NCI_MOV
# MAGIC FCD_FEH_CRE = Current Timestamp()
# MAGIC FCC_USU_CRE = FCC_USU_CRE
# MAGIC FCD_FEH_ACT = Current Timestamp()
# MAGIC FCC_USU_ACT = FCC_USU_ACT
# MAGIC FTN_DEDUCIBLE = FTN_DEDUCIBLE
# MAGIC FCN_ID_PLAZO = FCN_ID_PLAZO
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


# Verificar si p_SR_ETAPA es diferente de 2 antes de aplicar las reglas
if int(p_SR_ETAPA) != 2:
    # Filtrar las filas que cumplen con la condición
    DF_DISPERSIONES_03 = DF_02_DISPERSIONES.filter(
        (F.col("FTN_ID_SEMAFORO_MOV") == 190) & (F.col("FTN_ID_ERROR_VAL").isNull())
    )

    # Inserto DF_DISPERSIONES_03 al cache
    DF_DISPERSIONES_03.cache()

    # Aplicar las reglas al nuevo DataFrame DF_DISPERSIONES_03
    DF_DISPERSIONES_03 = (
        DF_DISPERSIONES_03.withColumn("FTN_ID_BAL_MOV", F.col("CONTADOR"))
        .withColumn("FTC_FOLIO", F.col("FTC_FOLIO"))
        .withColumn("FTC_FOLIO_REL", F.col("FTC_FOLIO_REL"))
        .withColumn("FTN_DISP_PESOS", F.lit(0))
        .withColumn(
            "FTN_DISP_ACCIONES",
            F.when(F.lit(p_SR_TIPO_MOV) == 2, F.lit(0)).otherwise(
                (F.col("FTF_MONTO_ACCIONES") * -1) * F.lit(p_SR_FACTOR)
            ),
        )
        .withColumn(
            "FTN_PDTE_PESOS",
            F.when(
                F.lit(p_SR_TIPO_MOV) == 2, F.col("FTF_MONTO_PESOS") * F.lit(p_SR_FACTOR)
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "FTN_PDTE_ACCIONES",
            F.when(
                F.lit(p_SR_TIPO_MOV) == 2,
                F.col("FTF_MONTO_ACCIONES") * F.lit(p_SR_FACTOR),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "FTN_COMP_PESOS",
            F.when(F.lit(p_SR_TIPO_MOV) == 2, F.lit(0)).otherwise(
                F.col("FTF_MONTO_PESOS") * F.lit(p_SR_FACTOR)
            ),
        )
        .withColumn(
            "FTN_COMP_ACCIONES",
            F.when(F.lit(p_SR_TIPO_MOV) == 2, F.lit(0)).otherwise(
                F.col("FTF_MONTO_ACCIONES") * F.lit(p_SR_FACTOR)
            ),
        )
        .withColumn("FTN_DIA_PESOS", F.lit(0))
        .withColumn("FTN_DIA_ACCIONES", F.lit(0))
        .withColumn("FTC_NUM_CTA_INVDUAL", F.col("FTC_NUM_CTA_INVDUAL"))
        .withColumn(
            "FTN_ORIGEN_APORTACION",
            F.when(F.col("FCN_ID_TIPO_SUBCTA").isin(19, 21, 23), F.lit(293)).otherwise(
                F.lit(None)
            ),
        )
        .withColumn("FCN_ID_TIPO_SUBCTA", F.col("FCN_ID_TIPO_SUBCTA"))
        .withColumn("FCN_ID_SIEFORE", F.col("FCN_ID_SIEFORE"))
        .withColumn("FCN_ID_VALOR_ACCION", F.col("FCN_ID_VALOR_ACCION"))
        .withColumn("FTD_FEH_LIQUIDACION", F.col("FTD_FEH_LIQUIDACION"))
        .withColumn("FCN_ID_TIPO_MOV", F.col("FCN_ID_TIPO_MOV"))
        .withColumn("FCN_ID_CONCEPTO_MOV", F.col("FCN_ID_CONCEPTO_MOV"))
        .withColumn("FCC_TABLA_NCI_MOV", F.col("FCC_TABLA_NCI_MOV"))
        .withColumn(
            "FCD_FEH_CRE",
            F.col("FCD_FEH_CRE"),
        )
        .withColumn("FCC_USU_CRE", F.col("FCC_USU_CRE"))
        .withColumn(
            "FCD_FEH_ACT",
            F.col("FCD_FEH_ACT"),
        )
        .withColumn("FCC_USU_ACT", F.col("FCC_USU_ACT"))
        .withColumn("FTN_DEDUCIBLE", F.col("FTN_DEDUCIBLE"))
        .withColumn("FCN_ID_PLAZO", F.col("FCN_ID_PLAZO"))
    )

    if debug:
        # Mostrar el resultado
        display(DF_DISPERSIONES_03)
        DF_DISPERSIONES_03.count()
    
    # unpersist DF_02_DISPERSIONES
    DF_02_DISPERSIONES.unpersist()
    del DF_02_DISPERSIONES

else:
    print(f"p_SR_ETAPA ({p_SR_ETAPA}) es igual a 2, no se aplicarán las reglas.")

# COMMAND ----------

from pyspark.sql import functions as F

if int(p_SR_ETAPA) != 2:
    # Lista de columnas que deseas seleccionar
    columns_to_select = [
        "FTN_ID_BAL_MOV",
        "FTC_FOLIO",
        "FTC_FOLIO_REL",
        "FTN_DISP_PESOS",
        "FTN_DISP_ACCIONES",
        "FTN_PDTE_PESOS",
        "FTN_PDTE_ACCIONES",
        "FTN_COMP_PESOS",
        "FTN_COMP_ACCIONES",
        "FTN_DIA_PESOS",
        "FTN_DIA_ACCIONES",
        "FTC_NUM_CTA_INVDUAL",
        "FTN_ORIGEN_APORTACION",
        "FCN_ID_TIPO_SUBCTA",
        "FCN_ID_SIEFORE",
        "FCN_ID_VALOR_ACCION",
        "FTD_FEH_LIQUIDACION",
        "FCN_ID_TIPO_MOV",
        "FCN_ID_CONCEPTO_MOV",
        "FCC_TABLA_NCI_MOV",
        "FCD_FEH_CRE",
        "FCC_USU_CRE",
        "FCD_FEH_ACT",
        "FCC_USU_ACT",
        "FTN_DEDUCIBLE",
        "FCN_ID_PLAZO",
    ]

    # Primero, casteamos la columna FTC_FOLIO a string
    DF_DISPERSIONES_03 = DF_DISPERSIONES_03.withColumn("FTC_FOLIO", F.col("FTC_FOLIO").cast("string"))

    # Luego, aplicamos el split para eliminar los decimales
    DF_DISPERSIONES_03 = DF_DISPERSIONES_03.withColumn("FTC_FOLIO", F.split(F.col("FTC_FOLIO"), "\.")[0])

    # Seleccionar las columnas especificadas y reemplazar el DataFrame original
    DF_DISPERSIONES_03 = DF_DISPERSIONES_03.select(*columns_to_select)

    if debug:
        display(DF_DISPERSIONES_03)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Renombramos por temas de documentacion el DF a `DF_260_REGS_ACTUALES_02_6`

# COMMAND ----------

if int(p_SR_ETAPA) != 2:
    DF_260_REGS_ACTUALES_02_6 = DF_DISPERSIONES_03
    DF_DISPERSIONES_03.unpersist()
    del DF_DISPERSIONES_03

# COMMAND ----------

# MAGIC %md
# MAGIC ### Leemos `oracle_dispersiones` (Unity Catalog) y lo asignamos a `DF_DISPERSIONES_03`

# COMMAND ----------

if int(p_SR_ETAPA) != 2:
    catalog_name = f"{global_confs['catalog_name']}"
    schema_name = f"{global_confs['schema_name']}"
    table_name = f"oracle_dispersiones_{global_params['SR_FOLIO']}"

    try:
        # Create the DataFrame DF_DISPERSIONES_03 from the temporary view TEMP_DISPERSION_MOV_02
        DF_DISPERSIONES_03_ = spark.sql(
            f"SELECT * FROM {catalog_name}.{schema_name}.{table_name}"
        )

        # Inserto DF_02_DISPERSIONES al cache
        DF_DISPERSIONES_03_.cache()

        if debug:
            display(DF_DISPERSIONES_03_)
            display(DF_260_REGS_ACTUALES_02_6)
    except Exception as e:
        logger.error("Revisar mensajes en los logs")
        notification_raised(webhook_url, -1, message, source, input_parameters)
        raise Exception(
            message
        )

# COMMAND ----------

# MAGIC %md
# MAGIC Hacemos un left join entre `DF_260_REGS_ACTUALES_02_6` y `DF_DISPERSIONES_03` a traves de las keys `FTC_FOLIO` y `FTC_FOLIO_REL` y los registros que no hagan match los almacenamos en `DF_250_REGS_ANT_02_5`.

# COMMAND ----------

if int(p_SR_ETAPA) != 2:
    # Verifica los tipos de datos de las columnas clave
    DF_260_REGS_ACTUALES_02_6.select("FTC_FOLIO", "FTC_FOLIO_REL").printSchema()
    DF_DISPERSIONES_03_.select("FTC_FOLIO", "FTC_FOLIO_REL").printSchema()

    # Si las columnas clave no tienen el mismo tipo de datos, conviértalas a string
    DF_260_REGS_ACTUALES_02_6 = DF_260_REGS_ACTUALES_02_6.withColumn(
        "FTC_FOLIO", DF_260_REGS_ACTUALES_02_6["FTC_FOLIO"].cast("string")
    ).withColumn(
        "FTC_FOLIO_REL", DF_260_REGS_ACTUALES_02_6["FTC_FOLIO_REL"].cast("string")
    )

    DF_DISPERSIONES_03_ = DF_DISPERSIONES_03_.withColumn(
        "FTC_FOLIO", DF_DISPERSIONES_03_["FTC_FOLIO"].cast("string")
    ).withColumn(
        "FTC_FOLIO_REL", DF_DISPERSIONES_03_["FTC_FOLIO_REL"].cast("string")
    )

    # Realizar el left anti join para capturar los registros que no hacen match
    DF_250_REGS_ANT_02_5 = DF_DISPERSIONES_03_.join(
        DF_260_REGS_ACTUALES_02_6,
        (DF_DISPERSIONES_03_.FTC_FOLIO == DF_260_REGS_ACTUALES_02_6.FTC_FOLIO)
        & (DF_DISPERSIONES_03_.FTC_FOLIO_REL == DF_260_REGS_ACTUALES_02_6.FTC_FOLIO_REL),
        how="left_anti"
    )

    # Cachear DF_250_REGS_ANT_02_5 para optimizar futuras consultas
    DF_250_REGS_ANT_02_5.cache()

    if debug:
        # Mostrar el esquema y los datos para depurar
        DF_250_REGS_ANT_02_5.printSchema()
        display(DF_250_REGS_ANT_02_5)

        # Contar los registros y el número de columnas
        count = DF_250_REGS_ANT_02_5.count()
        num_columns = len(DF_250_REGS_ANT_02_5.columns)
        print(f"Cantidad de registros que no hacen match: {count}")
        print(f"Cantidad de columnas del schema: {num_columns}")


# COMMAND ----------

# MAGIC %md
# MAGIC Hacemos un union entre el `DF_260_REGS_ACTUALES_02_6` y el `DF_250_REGS_ANT_02_5` (`JP_PANCIN_MOV_0025_GENERA_DS_BAL`)

# COMMAND ----------

# DBTITLE 1,COMPARACION DE SCHEMAS DE DF
if int(p_SR_ETAPA) != 2:
    if debug:
        # Compare schemas between DF_260_REGS_ACTUALES_02_6 and DF_250_REGS_ANT_02_5
        schema_260 = DF_260_REGS_ACTUALES_02_6.schema
        schema_250 = DF_250_REGS_ANT_02_5.schema

        schema_comparison = [
            (
                field_260.name,
                field_260.dataType,
                field_250.name,
                field_250.dataType,
                field_260.name == field_250.name
                and field_260.dataType == field_250.dataType,
            )
            for field_260, field_250 in zip(schema_260, schema_250)
        ]

        # Create a DataFrame to display the schema comparison
        schema_comparison_df = spark.createDataFrame(
            schema_comparison,
            ["DF_260_Name", "DF_260_Type", "DF_250_Name", "DF_250_Type", "Is_Equal"],
        )

        display(schema_comparison_df)

# COMMAND ----------

if int(p_SR_ETAPA) != 2:
    # Perform a simple union between DF_260_REGS_ACTUALES_02_6 and DF_250_REGS_ANT_02_5
    DF_UNION = DF_260_REGS_ACTUALES_02_6.union(DF_250_REGS_ANT_02_5)

    if debug:
        display(DF_UNION)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reemplazamos los datos en `ORACLE_DISPERSIONES`

# COMMAND ----------

from pyspark.sql.utils import AnalysisException
import time

# Número máximo de reintentos
max_retries = 5
# Intervalo entre reintentos (en segundos)
retry_interval = 60

# Nombre completo de la tabla
table_name = f"{global_confs['catalog_name']}.{global_confs['schema_name']}.oracle_dispersiones_{global_params['SR_FOLIO']}"

# Función para escribir los datos con reintento
def write_with_retries():
    retries = 0
    while retries < max_retries:
        try:
            if int(p_SR_ETAPA) != 2:
                # Crear o actualizar la tabla de manera segura
                spark.sql(f"DROP TABLE IF EXISTS {table_name}")
                DF_UNION.write.format("delta").mode("overwrite").saveAsTable(table_name)

                if debug:
                    spark.sql(f"SELECT * FROM {table_name}").show()

            print(f"Escritura completada en la tabla {table_name}")
            return
        except AnalysisException as e:
            retries += 1
            print(f"Error al escribir en la tabla {table_name}: {str(e)}")
            print(f"Reintentando... ({retries}/{max_retries})")
            time.sleep(retry_interval)
        except Exception as e:
            print(f"Error inesperado: {str(e)}")
            raise
    logger.error("Revisar mensajes en los logs")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise RuntimeError(f"Error persistente al escribir en la tabla {table_name} después de {max_retries} reintentos")

# Llama a la función con reintento
write_with_retries()


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

if int(p_SR_ETAPA) in [1, 3]:
    # Liberar la caché del DataFrame si se usó cache
    DF_DISPERSIONES_03_.unpersist()

    # Eliminar DataFrames para liberar memoria
    del DF_DISPERSIONES_03_

    # Recolector de basura para liberar recursos inmediatamente
    import gc
    gc.collect()
