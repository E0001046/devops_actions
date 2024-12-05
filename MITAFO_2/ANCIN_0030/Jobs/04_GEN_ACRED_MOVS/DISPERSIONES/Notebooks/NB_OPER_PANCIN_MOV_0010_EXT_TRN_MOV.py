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
# MAGIC     }
# MAGIC
# MAGIC     # Crear los widgets en minúsculas
# MAGIC     for key, default_value in widget_defaults.items():
# MAGIC         dbutils.widgets.text(key.lower(), default_value)
# MAGIC
# MAGIC     # Actualizar el diccionario global en mayúsculas para el resto del notebook
# MAGIC     global_params.update(
# MAGIC         {
# MAGIC             key.upper(): dbutils.widgets.get(key.lower()).strip()
# MAGIC             for key in widget_defaults
# MAGIC         }
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
# MAGIC     debug = debug.lower() == "true"

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
# MAGIC Creo `TEMP_DISPERSION_MOV_01_DF` a partir de su vista global homonima y calculo las siguientes variables:
# MAGIC
# MAGIC     1. `MONTOIMSSA`:
# MAGIC         1. IF FTC_TABLA_NCI_MOV = 'VIV' THEN DecimalToDecimal(FTF_MONTO_ACCIONES,"round_inf") ELSE DecimalToDecimal(FTF_MONTO_PESOS/ FCN_VALOR_ACCION,"round_inf")
# MAGIC     2. `MONTOISSSTEA`:
# MAGIC         1. IF FTC_TABLA_NCI_MOV = 'VIV' THEN DecimalToDecimal(FTF_MONTO_ACCIONES,"round_inf") ELSE DecimalToDecimal(FTF_MONTO_PESOS/ FCN_VALOR_ACCION,"round_inf")
# MAGIC     3. `MONTOIMSSC`:
# MAGIC         1. FTF_MONTO_ACCIONES
# MAGIC     4. `MONTOISSSTEC`:
# MAGIC         1. DecimalToDecimal(FTF_MONTO_ACCIONES,"round_inf")

# COMMAND ----------

from pyspark.sql.types import DecimalType

TEMP_DISPERSION_MOV_01_DF = spark.sql(
    f"SELECT * FROM {global_confs['catalog_name']}.{global_confs['schema_name']}.temp_dispersion_mov_01_{global_params['SR_FOLIO']}"
)

# Inserto TEMP_DISPERSION_MOV_01_DF al cache
TEMP_DISPERSION_MOV_01_DF.cache()

# TEMP_DISPERSION_MOV_01_DF = TEMP_DISPERSION_MOV_01_DF.withColumn(
#     "FTF_MONTO_PESOS",
#     TEMP_DISPERSION_MOV_01_DF["FTF_MONTO_PESOS"].cast(DecimalType(18, 2)),
# )

# TEMP_DISPERSION_MOV_01_DF = TEMP_DISPERSION_MOV_01_DF.withColumn(
#     "FCN_VALOR_ACCION",
#     TEMP_DISPERSION_MOV_01_DF["FCN_VALOR_ACCION"].cast(DecimalType(18, 6)),
# )

# TEMP_DISPERSION_MOV_01_DF = TEMP_DISPERSION_MOV_01_DF.withColumn(
#     "FTF_MONTO_ACCIONES",
#     TEMP_DISPERSION_MOV_01_DF["FTF_MONTO_ACCIONES"].cast(DecimalType(18, 6)),
# )

if debug:
    display(TEMP_DISPERSION_MOV_01_DF)

# COMMAND ----------

from pyspark.sql.functions import col, round, expr
from pyspark.sql.types import DecimalType, DoubleType
from pyspark.sql.functions import col, expr, regexp_replace
from pyspark.sql import functions as F

TEMP_DISPERSION_MOV_01_DF = TEMP_DISPERSION_MOV_01_DF.withColumn(
    "MONTOIMSSA_AUX",
    F.round(col("FTF_MONTO_PESOS").cast(DoubleType())
    / col("FCN_VALOR_ACCION").cast(DoubleType()),10),
).withColumn(
    "MONTOISSSTEA_AUX",
    F.round(col("FTF_MONTO_PESOS").cast(DoubleType())
    / col("FCN_VALOR_ACCION").cast(DoubleType()),10),
)

# TEMP_DISPERSION_MOV_01_DF = TEMP_DISPERSION_MOV_01_DF.withColumn(
#     "MONTOIMSSA_AUX",
#     col("MONTOIMSSA_AUX").cast(DecimalType(38, 8)),
# ).withColumn(
#     "MONTOISSSTEA_AUX",
#     col("MONTOISSSTEA_AUX").cast(DecimalType(38, 8)),
# )

# TEMP_DISPERSION_MOV_01_DF = TEMP_DISPERSION_MOV_01_DF.withColumn(
#     "septimo_decimal", expr("floor((MONTOIMSSA_AUX * 10000000) % 10)")
# )

# TEMP_DISPERSION_MOV_01_DF = TEMP_DISPERSION_MOV_01_DF.withColumn(
#     "octavo_decimal", expr("floor((MONTOIMSSA_AUX * 100000000) % 10)")
# )

# TEMP_DISPERSION_MOV_01_DF = TEMP_DISPERSION_MOV_01_DF.withColumn(
#     "noveno_decimal", expr("floor((MONTOIMSSA_AUX * 1000000000) % 10)")
# )

# # Create a new column with the decimal part after the seventh decimal
# TEMP_DISPERSION_MOV_01_DF = TEMP_DISPERSION_MOV_01_DF.withColumn(
#     "decimal_part_after_seventh",
#     expr(
#         "substring(cast(MONTOIMSSA_AUX as string), instr(cast(MONTOIMSSA_AUX as string), '.') + 8, length(cast(MONTOIMSSA_AUX as string)))"
#     ),
# )

# # Remove leading zeros
# TEMP_DISPERSION_MOV_01_DF = TEMP_DISPERSION_MOV_01_DF.withColumn(
#     "decimal_part_after_seventh",
#     regexp_replace(col("decimal_part_after_seventh"), "^0+", ""),
# )

if debug:
    display(TEMP_DISPERSION_MOV_01_DF)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col, expr

# MONTOIMSSA
TEMP_DISPERSION_MOV_01_DF = TEMP_DISPERSION_MOV_01_DF.withColumn(
    "MONTOIMSSA",
    F.when(
        TEMP_DISPERSION_MOV_01_DF["FTC_TABLA_NCI_MOV"] == "VIV",
        F.round(TEMP_DISPERSION_MOV_01_DF["FTF_MONTO_ACCIONES"], 6),
    ).otherwise(
        # F.when(
        #     (col("septimo_decimal") == 5)
        #     & (F.length(col("decimal_part_after_seventh")) == 0),
        #     F.floor(col("MONTOIMSSA_AUX"), 6),  # redondeo hacia abajo normal
        # )
        # .when(
        #     (col("septimo_decimal") == 5)
        #     & (F.length(col("decimal_part_after_seventh")) != 0),
        #     F.floor(col("MONTOIMSSA_AUX") * 1000000) / 1000000,  # truncado
        # )
        # .otherwise(F.round(col("MONTOIMSSA_AUX"), 6)),  # redondeo nomal

        F.round(col("MONTOIMSSA_AUX"), 6) # REDONDEO NORMAL
        # col("MONTOIMSSA_AUX")
    ),
)

# MONTOISSSTEA
TEMP_DISPERSION_MOV_01_DF = TEMP_DISPERSION_MOV_01_DF.withColumn(
    "MONTOISSSTEA",
    F.when(
        TEMP_DISPERSION_MOV_01_DF["FTC_TABLA_NCI_MOV"] == "VIV",
        F.round(TEMP_DISPERSION_MOV_01_DF["FTF_MONTO_ACCIONES"], 6),
    ).otherwise(
        # F.when(
        #     (col("septimo_decimal") == 5)
        #     & (F.length(col("decimal_part_after_seventh")) == 0),
        #     F.floor(col("MONTOISSSTEA_AUX"), 6),
        # )
        # .when(
        #     (col("septimo_decimal") == 5)
        #     & (F.length(col("decimal_part_after_seventh")) != 0),z
        #     F.floor(col("MONTOISSSTEA_AUX") * 1000000) / 1000000,
        # )
        # .otherwise(F.round(col("MONTOISSSTEA_AUX"), 6)),

        F.round(col("MONTOIMSSA_AUX"), 6) # REDONDEO NORMAL
        # col("MONTOISSSTEA_AUX")
    ),
)

# MONTOIMSSC
TEMP_DISPERSION_MOV_01_DF = TEMP_DISPERSION_MOV_01_DF.withColumn(
    "MONTOIMSSC", TEMP_DISPERSION_MOV_01_DF["FTF_MONTO_ACCIONES"]
)

# MONTOISSSTEC
TEMP_DISPERSION_MOV_01_DF = TEMP_DISPERSION_MOV_01_DF.withColumn(
    "MONTOISSSTEC", F.round(TEMP_DISPERSION_MOV_01_DF["FTF_MONTO_ACCIONES"], 6)
)

if debug:
    display(TEMP_DISPERSION_MOV_01_DF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generar un df para registros semaforo rojo (IsNull(FCN_ID_VALOR_ACCION) OR IsNull(FTN_ID_MARCA) OR FTN_ID_ERROR_VAL = 283)

# COMMAND ----------

df_rojo = TEMP_DISPERSION_MOV_01_DF.filter(
    F.col("FCN_ID_VALOR_ACCION").isNull() |
    F.col("FTN_ID_MARCA").isNull() |
    (F.col("FTN_ID_ERROR_VAL") == 283)
)

# Inserto df_rojo al cache
df_rojo.cache()

if debug:
    display(df_rojo)

# COMMAND ----------

# MAGIC %md
# MAGIC A los registros de semaforo rojo se le aplican estas reglas
# MAGIC
# MAGIC ```text
# MAGIC FTN_ID_SEMAFORO_MOV: Se establece un valor constante de 191.
# MAGIC ID_MOV: Se incrementa la variable CONTADOR.
# MAGIC FTF_MONTO_PESOS: Se aplican las reglas condicionales basadas en p_SR_TIPO_MOV y INSTITUTO.
# MAGIC FTF_MONTO_ACCIONES: Se aplican las reglas condicionales utilizando p_SR_TIPO_MOV y INSTITUTO.
# MAGIC FCN_ID_VALOR_ACCION: Si el valor es nulo, se establece en 9999.
# MAGIC FTD_FEH_LIQUIDACION: Se convierte la fecha proporcionada en p_SR_FEC_LIQ a timestamp.
# MAGIC FTD_ID_ESTATUS_MOV: Se establece un valor constante de 758.
# MAGIC FTN_ID_MARCA: Si es nulo, se establece en 1111111111.
# MAGIC FCN_ID_TIPO_MOV: Se determina según p_SR_TIPO_MOV.
# MAGIC FCD_FEH_CRE y FCD_FEH_ACT: Ambas columnas se rellenan con la fecha y hora actuales.
# MAGIC FTN_REG_ACREDITADO y FTN_MOV_GENERADO: Ambas columnas se establecen en 0.
# MAGIC ```

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import datetime  # Import the datetime class
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    from_utc_timestamp,
    to_timestamp,
)

# Inicializar la variable CONTADOR
CONTADOR = 0

# Definir el parámetro p_SR_TIPO_MOV y p_SR_FEC_LIQ desde global_params
p_SR_TIPO_MOV = global_params["SR_TIPO_MOV"]
p_SR_FEC_LIQ = global_params["SR_FECHA_LIQ"]
p_SR_FEC_LIQ = datetime.strptime(p_SR_FEC_LIQ, "%Y%m%d").strftime("%Y-%m-%d")


# Agregar la columna ID_MOV con el valor del contador incrementado
# df_rojo = df_rojo.withColumn(
#     "ID_MOV",
#     F.monotonically_increasing_id() + 1  # Empezar el contador en 1
# )

# Aplicar las reglas al DataFrame df_rojo

# FTN_ID_SEMAFORO_MOV
df_rojo = df_rojo.withColumn("FTN_ID_SEMAFORO_MOV", F.lit(191))

# FTF_MONTO_PESOS
df_rojo = df_rojo.withColumn(
    "FTF_MONTO_PESOS",
    F.when(
        F.lit(p_SR_TIPO_MOV) == 2,
        F.round(df_rojo["FTF_MONTO_PESOS"], 2),
    )
    .when(df_rojo["INSTITUTO"] == "IMSS", F.round(df_rojo["FTF_MONTO_PESOS"], 2))
    .when(
        df_rojo["INSTITUTO"] == "ISSSTE",
        F.round(df_rojo["FCN_VALOR_ACCION"] * df_rojo["FTF_MONTO_ACCIONES"], 2),
    )
    .otherwise(F.lit(0)),
)

# FTF_MONTO_ACCIONES
df_rojo = df_rojo.withColumn(
    "FTF_MONTO_ACCIONES",
    F.when(
        F.lit(p_SR_TIPO_MOV) == 2,
        F.when(df_rojo["INSTITUTO"] == "IMSS", df_rojo["MONTOIMSSA"])
        .when(df_rojo["INSTITUTO"] == "ISSSTE", df_rojo["MONTOISSSTEA"])
        .otherwise(F.lit(0)),
    ).otherwise(
        F.when(df_rojo["INSTITUTO"] == "IMSS", df_rojo["MONTOIMSSC"])
        .when(df_rojo["INSTITUTO"] == "ISSSTE", df_rojo["MONTOISSSTEC"])
        .otherwise(F.lit(0))
    ),
)

# FCN_ID_VALOR_ACCION
df_rojo = df_rojo.withColumn(
    "FCN_ID_VALOR_ACCION",
    F.when(F.col("FCN_ID_VALOR_ACCION").isNull(), F.lit(9999)).otherwise(
        F.col("FCN_ID_VALOR_ACCION")
    ),
)

# FTD_FEH_LIQUIDACION
df_rojo = df_rojo.withColumn(
    "FTD_FEH_LIQUIDACION", F.to_timestamp(F.lit(p_SR_FEC_LIQ), "yyyy-MM-dd")
)

# FTD_ID_ESTATUS_MOV
df_rojo = df_rojo.withColumn("FTD_ID_ESTATUS_MOV", F.lit(758))

# FTN_ID_MARCA
df_rojo = df_rojo.withColumn(
    "FTN_ID_MARCA",
    F.when(F.col("FTN_ID_MARCA").isNull(), F.lit(1111111111)).otherwise(
        F.col("FTN_ID_MARCA")
    ),
)

# FCN_ID_TIPO_MOV
df_rojo = df_rojo.withColumn(
    "FCN_ID_TIPO_MOV",
    F.when(F.lit(p_SR_TIPO_MOV) == 1, F.lit(180)).otherwise(F.lit(181)),
)

# FCD_FEH_CRE y FCD_FEH_ACT
# Aplicar la misma lógica a las fechas FCD_FEH_CRE y FCD_FEH_ACT
current_timestamp_adjusted = to_timestamp(from_utc_timestamp(current_timestamp(), "GMT-6"))
df_rojo = df_rojo.withColumn("FCD_FEH_CRE", current_timestamp_adjusted)
df_rojo = df_rojo.withColumn("FCD_FEH_ACT", current_timestamp_adjusted)

# FTN_REG_ACREDITADO y FTN_MOV_GENERADO
df_rojo = df_rojo.withColumn("FTN_REG_ACREDITADO", F.lit(0))
df_rojo = df_rojo.withColumn("FTN_MOV_GENERADO", F.lit(0))

if debug:
    # Mostrar el resultado
    display(df_rojo)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Se genera un df para los registros semaforo verde (IsNotNull(FCN_ID_VALOR_ACCION) and IsNotNull(FTN_ID_MARCA) AND IsNull(FTN_ID_ERROR_VAL))

# COMMAND ----------

df_verde = TEMP_DISPERSION_MOV_01_DF.filter(
    F.col("FCN_ID_VALOR_ACCION").isNotNull() &
    F.col("FTN_ID_MARCA").isNotNull() &
    F.col("FTN_ID_ERROR_VAL").isNull()
)

# Inserto df_verde al cache
df_verde.cache()

if debug:
    display(df_verde)

# COMMAND ----------

# MAGIC %md
# MAGIC Se aplican a los registros semaforo verde estas reglas:
# MAGIC
# MAGIC ```
# MAGIC FTN_ID_SEMAFORO_MOV: Se establece un valor constante de 190.
# MAGIC ID_MOV: Se incrementa la variable CONTADOR para representar el ID de movimiento.
# MAGIC FTF_MONTO_PESOS: Se aplican las reglas condicionales según el valor de p_SR_TIPO_MOV y INSTITUTO.
# MAGIC FTF_MONTO_ACCIONES: Se definen los valores basados en p_SR_TIPO_MOV y INSTITUTO.
# MAGIC FCN_ID_VALOR_ACCION: Se mantiene el valor actual de FCN_ID_VALOR_ACCION.
# MAGIC FTD_FEH_LIQUIDACION: Se convierte la fecha proporcionada en p_SR_FEC_LIQ a timestamp.
# MAGIC FTD_ID_ESTATUS_MOV: Se establece un valor constante de 136.
# MAGIC FTN_ID_MARCA: Si es nulo, se establece en 1111111111.
# MAGIC FCN_ID_TIPO_MOV: Se determina según p_SR_TIPO_MOV.
# MAGIC FCD_FEH_CRE y FCD_FEH_ACT: Ambas columnas se rellenan con la fecha y hora actuales.
# MAGIC FTN_REG_ACREDITADO y FTN_MOV_GENERADO: Ambas columnas se establecen en 0.
# MAGIC ```

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import datetime  # Import the datetime class
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    from_utc_timestamp,
    to_timestamp,
)

# Inicializar la variable CONTADOR
CONTADOR = 0

# Definir el parámetro p_SR_TIPO_MOV y p_SR_FEC_LIQ desde global_params
p_SR_TIPO_MOV = global_params["SR_TIPO_MOV"]
p_SR_FEC_LIQ = global_params["SR_FECHA_LIQ"]
p_SR_FEC_LIQ = datetime.strptime(p_SR_FEC_LIQ, "%Y%m%d").strftime("%Y-%m-%d")

# Agregar la columna ID_MOV con el valor del contador incrementado
# df_verde = df_verde.withColumn(
#     "ID_MOV",
#     F.monotonically_increasing_id() + 1  # Empezar el contador en 1
# )

# Aplicar las reglas al DataFrame df_verde

# FTN_ID_SEMAFORO_MOV
df_verde = df_verde.withColumn("FTN_ID_SEMAFORO_MOV", F.lit(190))

# FTF_MONTO_PESOS
df_verde = df_verde.withColumn(
    "FTF_MONTO_PESOS",
    F.when(F.lit(p_SR_TIPO_MOV) == 2, F.round(df_verde["FTF_MONTO_PESOS"], 2))
    .when(df_verde["INSTITUTO"] == "IMSS", F.round(df_verde["FTF_MONTO_PESOS"], 2))
    .when(
        df_verde["INSTITUTO"] == "ISSSTE",
        F.round(df_verde["FCN_VALOR_ACCION"] * df_verde["FTF_MONTO_ACCIONES"], 2),
    )
    .otherwise(F.lit(0)),
)

# FTF_MONTO_ACCIONES
df_verde = df_verde.withColumn(
    "FTF_MONTO_ACCIONES",
    F.when(
        F.lit(p_SR_TIPO_MOV) == 2,
        F.when(df_verde["INSTITUTO"] == "IMSS", df_verde["MONTOIMSSA"])
        .when(df_verde["INSTITUTO"] == "ISSSTE", df_verde["MONTOISSSTEA"])
        .otherwise(F.lit(0)),
    ).otherwise(
        F.when(df_verde["INSTITUTO"] == "IMSS", df_verde["MONTOIMSSC"])
        .when(df_verde["INSTITUTO"] == "ISSSTE", df_verde["MONTOISSSTEC"])
        .otherwise(F.lit(0))
    ),
)

# FCN_ID_VALOR_ACCION
df_verde = df_verde.withColumn("FCN_ID_VALOR_ACCION", F.col("FCN_ID_VALOR_ACCION"))

# FTD_FEH_LIQUIDACION
df_verde = df_verde.withColumn(
    "FTD_FEH_LIQUIDACION", F.to_timestamp(F.lit(p_SR_FEC_LIQ), "yyyy-MM-dd")
)

# FTD_ID_ESTATUS_MOV
df_verde = df_verde.withColumn("FTD_ID_ESTATUS_MOV", F.lit(136))

# FTN_ID_MARCA
df_verde = df_verde.withColumn(
    "FTN_ID_MARCA",
    F.when(F.col("FTN_ID_MARCA").isNull(), F.lit(1111111111)).otherwise(
        F.col("FTN_ID_MARCA")
    ),
)

# FCN_ID_TIPO_MOV
df_verde = df_verde.withColumn(
    "FCN_ID_TIPO_MOV",
    F.when(F.lit(p_SR_TIPO_MOV) == 1, F.lit(180)).otherwise(F.lit(181)),
)

# FCD_FEH_CRE y FCD_FEH_ACT
# Aplicar la misma lógica a las fechas FCD_FEH_CRE y FCD_FEH_ACT
current_timestamp_adjusted = to_timestamp(from_utc_timestamp(current_timestamp(), "GMT-6"))
df_verde = df_verde.withColumn("FCD_FEH_CRE", current_timestamp_adjusted)
df_verde = df_verde.withColumn("FCD_FEH_ACT", current_timestamp_adjusted)

# FTN_REG_ACREDITADO y FTN_MOV_GENERADO
df_verde = df_verde.withColumn("FTN_REG_ACREDITADO", F.lit(0))
df_verde = df_verde.withColumn("FTN_MOV_GENERADO", F.lit(0))

if debug:
    # Mostrar el resultado
    display(df_verde)

# COMMAND ----------

# MAGIC %md
# MAGIC > Hacemos la union de `df_rojo` y `df_verde` y la almacenamos en `TEMP_DISPERSION_MOV_01_DF` y le agregamos la columna `FTN_ID_MOV` (Es como un RowNumber())

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Union de df_rojo y df_verde
TEMP_DISPERSION_MOV_01_DF = df_rojo.unionByName(df_verde, allowMissingColumns=True)

# unpersist df_rojo, df_rojo
df_rojo.unpersist()
df_verde.unpersist()
del df_rojo, df_verde

# Definir la ventana para el ordenamiento
windowSpec = Window.orderBy(F.lit(1))

# Agregar la columna FTN_ID_MOV con valores consecutivos comenzando desde 1
TEMP_DISPERSION_MOV_01_DF = TEMP_DISPERSION_MOV_01_DF.withColumn(
    "FTN_ID_MOV",
    F.row_number().over(windowSpec)
)

if debug:
    # Mostrar el resultado
    display(TEMP_DISPERSION_MOV_01_DF)


# COMMAND ----------

# MAGIC %md
# MAGIC # Nota:
# MAGIC A partir de este punto el job se considera la primera parte de la sub etapa `JP_PANCIN_MOV_0020_INS_MOV_BAL_BALANCE`

# COMMAND ----------

# MAGIC %md
# MAGIC Hacemos una extraccion a `CIERREN.TTAFOGRAL_BALANCE_MOVS` y a `CIERREN_ETL.TTSISGRAL_ETL_MOVIMIENTOS`

# COMMAND ----------

query_statement = "007"

params = [
]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == "0":
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, global_params)
    raise Exception("Process ends")

# Ensure the statement is correctly formatted with the parameters
formatted_statement = statement.format(*params)

DF_500_MAX_TL_MOV_AND_BAL, failed_task = query_table(
    conn_name_ora, spark, formatted_statement, conn_options, conn_user, conn_key
)

# Insertar DF_500_MAX_TL_MOV_AND_BAL al cache
DF_500_MAX_TL_MOV_AND_BAL.cache()

if failed_task == "0":
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")

if debug:
    display(DF_500_MAX_TL_MOV_AND_BAL)

# COMMAND ----------

# MAGIC %md
# MAGIC A `DF_02_DISPERSIONES` le agragamos la columna LLAVE y esta va a tener un valor estatico de 1

# COMMAND ----------

from pyspark.sql.functions import lit
from pyspark.sql.types import DecimalType

# Define el tipo de dato Decimal(38, 10)
decimal_type = DecimalType(38, 10)

# Agrega la columna LLAVE con un valor estático de 1 y tipo Decimal(38, 10)
DF_02_DISPERSIONES = TEMP_DISPERSION_MOV_01_DF.withColumn("LLAVE", lit(1).cast(decimal_type))

# unpersist TEMP_DISPERSION_MOV_01_DF
TEMP_DISPERSION_MOV_01_DF.unpersist()

# del TEMP_DISPERSION_MOV_01_DF
del TEMP_DISPERSION_MOV_01_DF

# Insertamos DF_02_DISPERSIONES al cache
DF_02_DISPERSIONES.cache()

if debug:
    display(DF_02_DISPERSIONES)

# COMMAND ----------

# MAGIC %md
# MAGIC Hacemos un inner join entre `DF_02_DISPERSIONES` y el `DF_500_MAX_TL_MOV_AND_BAL` a traves de la llave key LLAVE.

# COMMAND ----------

DF_02_DISPERSIONES = DF_02_DISPERSIONES.join(DF_500_MAX_TL_MOV_AND_BAL, on="LLAVE", how="left")

# unpersist DF_500_MAX_TL_MOV_AND_BAL
DF_500_MAX_TL_MOV_AND_BAL.unpersist()

# del DF_500_MAX_TL_MOV_AND_BAL
del DF_500_MAX_TL_MOV_AND_BAL

if debug:
    display(DF_02_DISPERSIONES)
    DF_02_DISPERSIONES.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Declaro estas variables que me sirviran para aplicar algunas reglas:
# MAGIC - **ID =** se inicializa en cero IF FTN_ID_SEMAFORO_MOV =  190 THEN ID + 1 ELSE ID
# MAGIC - **CONTADOR =** se inicializa en cero IF FTN_ID_SEMAFORO_MOV =  190 THEN MAXIMO1 + ID ELSE CONTADOR
# MAGIC - **ID2 =** se inicializa en cero y su criterio de cambio es ID2 + 1
# MAGIC - **CONTADOR2 =** se inicializa en cero y su creterio de cambio es MAXIMO2 + ID2
# MAGIC - **SALPESOS =** FTF_MONTO_PESOS
# MAGIC - **SALACCIONES =** FTF_MONTO_ACCIONES

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

ID = 0
CONTADOR = 0
ID2 = 0
CONTADOR2 = 0

# Ventanas para las operaciones de ID y ID2
windowSpecID = Window.partitionBy("FTN_ID_SEMAFORO_MOV").orderBy("FTN_ID_SEMAFORO_MOV")
windowSpecID2 = Window.orderBy("FTN_ID_SEMAFORO_MOV")

# Crear la columna ID según la regla con row_number para garantizar que sea contiguo
DF_02_DISPERSIONES = DF_02_DISPERSIONES.withColumn(
    "ID",
    F.when(
        F.col("FTN_ID_SEMAFORO_MOV") == 190, F.row_number().over(windowSpecID)
    ).otherwise(F.lit(ID)),
)

# Crear la columna CONTADOR según la regla
DF_02_DISPERSIONES = DF_02_DISPERSIONES.withColumn(
    "CONTADOR",
    F.when(
        F.col("FTN_ID_SEMAFORO_MOV") == 190, F.col("MAXIMO1") + F.col("ID")
    ).otherwise(F.lit(CONTADOR)),
)

# Crear la columna ID2 con row_number para garantizar contigüidad
DF_02_DISPERSIONES = DF_02_DISPERSIONES.withColumn(
    "ID2", F.row_number().over(windowSpecID2)
)

# Crear la columna CONTADOR2 según la regla
DF_02_DISPERSIONES = DF_02_DISPERSIONES.withColumn(
    "CONTADOR2", F.col("MAXIMO2") + F.col("ID2")
)

# Crear la columna SALPESOS
DF_02_DISPERSIONES = DF_02_DISPERSIONES.withColumn("SALPESOS", F.col("FTF_MONTO_PESOS"))

# Crear la columna SALACCIONES
DF_02_DISPERSIONES = DF_02_DISPERSIONES.withColumn(
    "SALACCIONES", F.col("FTF_MONTO_ACCIONES")
)

# Primero, casteamos la columna FTC_FOLIO a string
DF_02_DISPERSIONES = DF_02_DISPERSIONES.withColumn("FTC_FOLIO", F.col("FTC_FOLIO").cast("string"))

# Luego, aplicamos el split para eliminar los decimales
DF_02_DISPERSIONES = DF_02_DISPERSIONES.withColumn("FTC_FOLIO", F.split(F.col("FTC_FOLIO"), "\.")[0])

if debug:
    display(DF_02_DISPERSIONES)



# COMMAND ----------

# MAGIC %md
# MAGIC ### Guardamos `DF_02_DISPERSIONES` en unity catalog en una tabla llamada `TEMP_DISPERSION_MOV_02` (Tabla que ocupa Irving como catalogo)

# COMMAND ----------

table_name = f"TEMP_DISPERSION_MOV_{global_params['SR_FOLIO']}"

spark.sql(f"DROP TABLE IF EXISTS {global_confs['catalog_name']}.{global_confs['schema_name']}.{table_name}")
DF_02_DISPERSIONES.write.format("delta").mode("overwrite").saveAsTable(
    f"{global_confs['catalog_name']}.{global_confs['schema_name']}.{table_name}"
)

if debug:
    spark.sql(f"SELECT * FROM {global_confs['catalog_name']}.{global_confs['schema_name']}.{table_name}").show()

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

# MAGIC %md
# MAGIC ### Verificar si la tabla `oracle_dispersiones_{sr_folio}` existe y guardar la verificacion en una variable de del WF.

# COMMAND ----------

sr_folio = global_params['SR_FOLIO']
table_name = f"oracle_dispersiones_{sr_folio}"

# Verificar si la tabla existe
table_exists = spark.catalog.tableExists(f"{global_confs['catalog_name']}.{global_confs['schema_name']}.{table_name}")

# Crear la variable de WF
oracle_dispersiones_exists = "True" if table_exists else "False"
dbutils.jobs.taskValues.set(key="oracle_dispersiones_exists", value=oracle_dispersiones_exists)
logger.info(f"La tabla {table_name} existe: {oracle_dispersiones_exists}")

# COMMAND ----------

# Liberar la caché del DataFrame si se usó cache
DF_02_DISPERSIONES.unpersist()

# Eliminar DataFrames para liberar memoria
del DF_02_DISPERSIONES

# Recolector de basura para liberar recursos inmediatamente
import gc
gc.collect()
