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
# MAGIC         "CX_CRE_ESQUEMA": "CIERREN_ETL",
# MAGIC         "TL_CRE_DISPERSION": "TTSISGRAL_ETL_DISPERSION",
# MAGIC         "SR_ETAPA": "",
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
# MAGIC         "table_001",
# MAGIC         "table_002",
# MAGIC         "table_003",
# MAGIC         "table_004",
# MAGIC         "table_005",
# MAGIC         "table_006",
# MAGIC         "table_007",
# MAGIC         "table_008",
# MAGIC         "table_009",
# MAGIC         "table_010",
# MAGIC         "table_011",
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
# MAGIC     input_parameters = {}  # Puedes definir los parámetros relevantes aquí
# MAGIC
# MAGIC     # Registro del error y notificación
# MAGIC     logger.error("Please review log messages")
# MAGIC     notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC     raise Exception("An error raised")
# MAGIC
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
# MAGIC
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
# MAGIC ### Hacemos un query que tiene multiples extracciones y transformaciones en OCI

# COMMAND ----------

query_statement = "009"
table_name_001 = f"{global_confs['conn_schema_002']}.{global_confs['table_006']}" # CIERREN.TRAFOGRAL_MOV_SUBCTA
table_name_002 = f"{global_confs['conn_schema_002']}.{global_confs['table_007']}" # CIERREN.TTAFOGRAL_MATRIZ_CONVIVENCIA
table_name_003 = f"{global_confs['conn_schema_002']}.{global_confs['table_008']}" # CIERREN.TFAFOGRAL_CONFIG_CONCEP_MOV
table_name_004 = f"{global_confs['conn_schema_001']}.{global_confs['table_010']}" # CIERREN_ETL.TTSISGRAL_ETL_DISPERSION
table_name_005 = f"{global_confs['conn_schema_002']}.{global_confs['table_009']}" # CIERREN.TCAFOGRAL_VALOR_ACCION

params = [
    global_params["CX_CRE_ESQUEMA"],
    global_params["TL_CRE_DISPERSION"],
    table_name_001,
    table_name_002,
    table_name_003,
    global_params["SR_FOLIO"],
    global_params["SR_ETAPA"],
    global_params["SR_FOLIO_REL"],
    table_name_004,
    table_name_005,
    global_params["SR_FECHA_ACC"],
]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == "0":
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, global_params)
    raise Exception("Process ends")

# Ensure the statement is correctly formatted with the parameters
formatted_statement = statement.format(*params)

DF_100_TTSISGRAL_ETL_DISPERSION, failed_task = query_table(
    conn_name_ora, spark, formatted_statement, conn_options, conn_user, conn_key
)

# Inserto DF_100_CRE_ETL_DISPERSION al cache
DF_100_TTSISGRAL_ETL_DISPERSION.cache()

if failed_task == "0":
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")

if debug:
    display(DF_100_TTSISGRAL_ETL_DISPERSION)

# COMMAND ----------

# DBTITLE 1,DEV / TESTING / SUPPORT
# if debug:
#     from pyspark.sql import Row

#     first_record = DF_100_TTSISGRAL_ETL_DISPERSION.head(1)

#     if not first_record:
#         # Crear una lista de Rows con los datos estáticos
#         data = [
#             Row(
#                 TOTAL_ACCIONES=1000.50,
#                 TOTAL_PESOS=2000.75,
#                 FCN_ID_TIPO_SUBCTA=1,
#                 FTC_FOLIO=f"{global_params['SR_FOLIO']}",
#                 FTC_FOLIO_REL=f"{global_params['SR_FOLIO_REL']}",
#                 FCN_ID_SIEFORE=101,
#                 FTF_MONTO_PESOS=2000.75,
#                 FTF_MONTO_ACCIONES=1000.50,
#                 FCN_ID_VALOR_ACCION=10.25,
#                 FTN_NUM_CTA_INVDUAL="NUM_CTA_1",
#                 FTN_ID_MARCA=1,
#                 FTD_FEC_CRE="2024-01-01",
#                 FTC_USU_CRE="user1",
#                 FTD_FEC_ACT="2024-01-02",
#                 FTC_USU_ACT="user2",
#                 FFN_ID_CONCEPTO_MOV=637,
#                 FTC_TABLA_NCI_MOV="VIV",
#                 FNN_ID_REFERENCIA=12345,
#                 FCN_VALOR_ACCION=10.25,
#                 FTN_DEDUCIBLE=0,
#                 INSTITUTO="IMSS",
#             ),
#             Row(
#                 TOTAL_ACCIONES=1100.60,
#                 TOTAL_PESOS=2100.85,
#                 FCN_ID_TIPO_SUBCTA=2,
#                 FTC_FOLIO=f"{global_params['SR_FOLIO']}",
#                 FTC_FOLIO_REL=f"{global_params['SR_FOLIO_REL']}",
#                 FCN_ID_SIEFORE=102,
#                 FTF_MONTO_PESOS=2100.85,
#                 FTF_MONTO_ACCIONES=1100.60,
#                 FCN_ID_VALOR_ACCION=10.30,
#                 FTN_NUM_CTA_INVDUAL="NUM_CTA_2",
#                 FTN_ID_MARCA=2,
#                 FTD_FEC_CRE="2024-01-03",
#                 FTC_USU_CRE="user3",
#                 FTD_FEC_ACT="2024-01-04",
#                 FTC_USU_ACT="user4",
#                 FFN_ID_CONCEPTO_MOV=638,
#                 FTC_TABLA_NCI_MOV="VIV",
#                 FNN_ID_REFERENCIA=12346,
#                 FCN_VALOR_ACCION=10.30,
#                 FTN_DEDUCIBLE=0,
#                 INSTITUTO="ISSSTE",
#             ),
#             Row(
#                 TOTAL_ACCIONES=1200.70,
#                 TOTAL_PESOS=2200.95,
#                 FCN_ID_TIPO_SUBCTA=3,
#                 FTC_FOLIO=f"{global_params['SR_FOLIO']}",
#                 FTC_FOLIO_REL=f"{global_params['SR_FOLIO_REL']}",
#                 FCN_ID_SIEFORE=103,
#                 FTF_MONTO_PESOS=2200.95,
#                 FTF_MONTO_ACCIONES=1200.70,
#                 FCN_ID_VALOR_ACCION=10.35,
#                 FTN_NUM_CTA_INVDUAL="NUM_CTA_3",
#                 FTN_ID_MARCA=3,
#                 FTD_FEC_CRE="2024-01-05",
#                 FTC_USU_CRE="user5",
#                 FTD_FEC_ACT="2024-01-06",
#                 FTC_USU_ACT="user6",
#                 FFN_ID_CONCEPTO_MOV=1002,
#                 FTC_TABLA_NCI_MOV="VIV",
#                 FNN_ID_REFERENCIA=12347,
#                 FCN_VALOR_ACCION=10.35,
#                 FTN_DEDUCIBLE=1,
#                 INSTITUTO="IMSS",
#             ),
#             Row(
#                 TOTAL_ACCIONES=1300.80,
#                 TOTAL_PESOS=2301.05,
#                 FCN_ID_TIPO_SUBCTA=4,
#                 FTC_FOLIO=f"{global_params['SR_FOLIO']}",
#                 FTC_FOLIO_REL=f"{global_params['SR_FOLIO_REL']}",
#                 FCN_ID_SIEFORE=104,
#                 FTF_MONTO_PESOS=2301.05,
#                 FTF_MONTO_ACCIONES=1300.80,
#                 FCN_ID_VALOR_ACCION=10.40,
#                 FTN_NUM_CTA_INVDUAL="NUM_CTA_4",
#                 FTN_ID_MARCA=4,
#                 FTD_FEC_CRE="2024-01-07",
#                 FTC_USU_CRE="user7",
#                 FTD_FEC_ACT="2024-01-08",
#                 FTC_USU_ACT="user8",
#                 FFN_ID_CONCEPTO_MOV=1003,
#                 FTC_TABLA_NCI_MOV="VIV",
#                 FNN_ID_REFERENCIA=12348,
#                 FCN_VALOR_ACCION=10.40,
#                 FTN_DEDUCIBLE=1,
#                 INSTITUTO="ISSSTE",
#             ),
#             Row(
#                 TOTAL_ACCIONES=1400.90,
#                 TOTAL_PESOS=2401.15,
#                 FCN_ID_TIPO_SUBCTA=5,
#                 FTC_FOLIO=f"{global_params['SR_FOLIO']}",
#                 FTC_FOLIO_REL=f"{global_params['SR_FOLIO_REL']}",
#                 FCN_ID_SIEFORE=105,
#                 FTF_MONTO_PESOS=2401.15,
#                 FTF_MONTO_ACCIONES=1400.90,
#                 FCN_ID_VALOR_ACCION=10.45,
#                 FTN_NUM_CTA_INVDUAL="NUM_CTA_5",
#                 FTN_ID_MARCA=5,
#                 FTD_FEC_CRE="2024-01-09",
#                 FTC_USU_CRE="user9",
#                 FTD_FEC_ACT="2024-01-10",
#                 FTC_USU_ACT="user10",
#                 FFN_ID_CONCEPTO_MOV=231,
#                 FTC_TABLA_NCI_MOV="VIV",
#                 FNN_ID_REFERENCIA=12349,
#                 FCN_VALOR_ACCION=10.45,
#                 FTN_DEDUCIBLE=0,
#                 INSTITUTO="IMSS",
#             ),
#             Row(
#                 TOTAL_ACCIONES=1500.00,
#                 TOTAL_PESOS=2501.25,
#                 FCN_ID_TIPO_SUBCTA=6,
#                 FTC_FOLIO=f"{global_params['SR_FOLIO']}",
#                 FTC_FOLIO_REL=f"{global_params['SR_FOLIO_REL']}",
#                 FCN_ID_SIEFORE=106,
#                 FTF_MONTO_PESOS=2501.25,
#                 FTF_MONTO_ACCIONES=1500.00,
#                 FCN_ID_VALOR_ACCION=10.50,
#                 FTN_NUM_CTA_INVDUAL="NUM_CTA_6",
#                 FTN_ID_MARCA=6,
#                 FTD_FEC_CRE="2024-01-11",
#                 FTC_USU_CRE="user11",
#                 FTD_FEC_ACT="2024-01-12",
#                 FTC_USU_ACT="user12",
#                 FFN_ID_CONCEPTO_MOV=637,
#                 FTC_TABLA_NCI_MOV="VIV",
#                 FNN_ID_REFERENCIA=12350,
#                 FCN_VALOR_ACCION=10.50,
#                 FTN_DEDUCIBLE=1,
#                 INSTITUTO="ISSSTE",
#             ),
#             Row(
#                 TOTAL_ACCIONES=1600.10,
#                 TOTAL_PESOS=2601.35,
#                 FCN_ID_TIPO_SUBCTA=7,
#                 FTC_FOLIO=f"{global_params['SR_FOLIO']}",
#                 FTC_FOLIO_REL=f"{global_params['SR_FOLIO_REL']}",
#                 FCN_ID_SIEFORE=107,
#                 FTF_MONTO_PESOS=2601.35,
#                 FTF_MONTO_ACCIONES=1600.10,
#                 FCN_ID_VALOR_ACCION=10.55,
#                 FTN_NUM_CTA_INVDUAL="NUM_CTA_7",
#                 FTN_ID_MARCA=7,
#                 FTD_FEC_CRE="2024-01-13",
#                 FTC_USU_CRE="user13",
#                 FTD_FEC_ACT="2024-01-14",
#                 FTC_USU_ACT="user14",
#                 FFN_ID_CONCEPTO_MOV=638,
#                 FTC_TABLA_NCI_MOV="VIV",
#                 FNN_ID_REFERENCIA=12351,
#                 FCN_VALOR_ACCION=10.55,
#                 FTN_DEDUCIBLE=0,
#                 INSTITUTO="IMSS",
#             ),
#             Row(
#                 TOTAL_ACCIONES=1700.20,
#                 TOTAL_PESOS=2701.45,
#                 FCN_ID_TIPO_SUBCTA=8,
#                 FTC_FOLIO=f"{global_params['SR_FOLIO']}",
#                 FTC_FOLIO_REL=f"{global_params['SR_FOLIO_REL']}",
#                 FCN_ID_SIEFORE=108,
#                 FTF_MONTO_PESOS=2701.45,
#                 FTF_MONTO_ACCIONES=1700.20,
#                 FCN_ID_VALOR_ACCION=10.60,
#                 FTN_NUM_CTA_INVDUAL="NUM_CTA_8",
#                 FTN_ID_MARCA=8,
#                 FTD_FEC_CRE="2024-01-15",
#                 FTC_USU_CRE="user15",
#                 FTD_FEC_ACT="2024-01-16",
#                 FTC_USU_ACT="user16",
#                 FFN_ID_CONCEPTO_MOV=1002,
#                 FTC_TABLA_NCI_MOV="VIV",
#                 FNN_ID_REFERENCIA=12352,
#                 FCN_VALOR_ACCION=10.60,
#                 FTN_DEDUCIBLE=1,
#                 INSTITUTO="ISSSTE",
#             ),
#             Row(
#                 TOTAL_ACCIONES=1800.30,
#                 TOTAL_PESOS=2801.55,
#                 FCN_ID_TIPO_SUBCTA=9,
#                 FTC_FOLIO=f"{global_params['SR_FOLIO']}",
#                 FTC_FOLIO_REL=f"{global_params['SR_FOLIO_REL']}",
#                 FCN_ID_SIEFORE=109,
#                 FTF_MONTO_PESOS=2801.55,
#                 FTF_MONTO_ACCIONES=1800.30,
#                 FCN_ID_VALOR_ACCION=10.65,
#                 FTN_NUM_CTA_INVDUAL="NUM_CTA_9",
#                 FTN_ID_MARCA=9,
#                 FTD_FEC_CRE="2024-01-17",
#                 FTC_USU_CRE="user17",
#                 FTD_FEC_ACT="2024-01-18",
#                 FTC_USU_ACT="user18",
#                 FFN_ID_CONCEPTO_MOV=1003,
#                 FTC_TABLA_NCI_MOV="VIV",
#                 FNN_ID_REFERENCIA=12353,
#                 FCN_VALOR_ACCION=10.65,
#                 FTN_DEDUCIBLE=0,
#                 INSTITUTO="IMSS",
#             ),
#             Row(
#                 TOTAL_ACCIONES=1900.40,
#                 TOTAL_PESOS=2901.65,
#                 FCN_ID_TIPO_SUBCTA=10,
#                 FTC_FOLIO=f"{global_params['SR_FOLIO']}",
#                 FTC_FOLIO_REL=f"{global_params['SR_FOLIO_REL']}",
#                 FCN_ID_SIEFORE=110,
#                 FTF_MONTO_PESOS=2901.65,
#                 FTF_MONTO_ACCIONES=1900.40,
#                 FCN_ID_VALOR_ACCION=10.70,
#                 FTN_NUM_CTA_INVDUAL="NUM_CTA_10",
#                 FTN_ID_MARCA=10,
#                 FTD_FEC_CRE="2024-01-19",
#                 FTC_USU_CRE="user19",
#                 FTD_FEC_ACT="2024-01-20",
#                 FTC_USU_ACT="user20",
#                 FFN_ID_CONCEPTO_MOV=231,
#                 FTC_TABLA_NCI_MOV="VIV",
#                 FNN_ID_REFERENCIA=12354,
#                 FCN_VALOR_ACCION=10.70,
#                 FTN_DEDUCIBLE=1,
#                 INSTITUTO="ISSSTE",
#             ),
#         ]

#         # Crear el DataFrame
#         DF_100_TTSISGRAL_ETL_DISPERSION = spark.createDataFrame(data)

#         # Mostrar el DataFrame
#         display(DF_100_TTSISGRAL_ETL_DISPERSION)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hacemos un query que tiene multiples extracciones y transformaciones en OCI

# COMMAND ----------

query_statement = "010"
table_name_001 = f"{global_confs['conn_schema_002']}.{global_confs['table_006']}" # CIERREN.TRAFOGRAL_MOV_SUBCTA
table_name_002 = f"{global_confs['conn_schema_002']}.{global_confs['table_007']}" # CIERREN.TTAFOGRAL_MATRIZ_CONVIVENCIA
table_name_003 = f"{global_confs['conn_schema_002']}.{global_confs['table_008']}" # CIERREN.TFAFOGRAL_CONFIG_CONCEP_MOV
table_name_004 = f"{global_confs['conn_schema_001']}.{global_confs['table_010']}" # CIERREN_ETL.TTSISGRAL_ETL_DISPERSION
table_name_005 = f"{global_confs['conn_schema_002']}.{global_confs['table_009']}" # CIERREN.TCAFOGRAL_VALOR_ACCION
table_name_006 = f"{global_confs['conn_schema_002']}.{global_confs['table_011']}" # CIERREN.TTAFOGRAL_BALANCE_MOVS

params = [
    table_name_006,
    table_name_004,
    table_name_001,
    table_name_002,
    table_name_003,
    global_params["SR_FOLIO"],
    global_params["SR_ETAPA"],
    global_params["SR_FOLIO_REL"],
]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == "0":
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, global_params)
    raise Exception("Process ends")

# Ensure the statement is correctly formatted with the parameters
formatted_statement = statement.format(*params)

DF_300_TTAFOGRAL_BALANCE_MOVS, failed_task = query_table(
    conn_name_ora, spark, formatted_statement, conn_options, conn_user, conn_key
)

# Inserto DF_100_CRE_ETL_DISPERSION al cache
DF_300_TTAFOGRAL_BALANCE_MOVS.cache()

if failed_task == "0":
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")

if debug:
    display(DF_300_TTAFOGRAL_BALANCE_MOVS)

# COMMAND ----------

# if debug:
#     from pyspark.sql import Row

#     first_record = DF_300_TTAFOGRAL_BALANCE_MOVS.head(1)

#     if not first_record:
#         # Crear una lista de Rows con los datos estáticos
#         data = [
#             Row(
#                 FTN_NUM_CTA_INVDUAL="NUM_CTA_1",
#                 FCN_ID_TIPO_SUBCTA=1,
#                 FCN_ID_SIEFORE=101,
#                 FTN_DEDUCIBLE=0,
#                 FTN_DISP_PESOS=2000.75,
#                 FTN_DISP_ACCIONES=1000.50,
#                 FCN_ID_PLAZO=1000,  # Sin plazo
#             ),
#             Row(
#                 FTN_NUM_CTA_INVDUAL="NUM_CTA_2",
#                 FCN_ID_TIPO_SUBCTA=2,
#                 FCN_ID_SIEFORE=102,
#                 FTN_DEDUCIBLE=0,
#                 FTN_DISP_PESOS=2100.85,
#                 FTN_DISP_ACCIONES=1100.60,
#                 FCN_ID_PLAZO=12,
#             ),
#             Row(
#                 FTN_NUM_CTA_INVDUAL="NUM_CTA_3",
#                 FCN_ID_TIPO_SUBCTA=3,
#                 FCN_ID_SIEFORE=103,
#                 FTN_DEDUCIBLE=1,
#                 FTN_DISP_PESOS=2200.95,
#                 FTN_DISP_ACCIONES=1200.70,
#                 FCN_ID_PLAZO=1000,  # Sin plazo
#             ),
#             Row(
#                 FTN_NUM_CTA_INVDUAL="NUM_CTA_4",
#                 FCN_ID_TIPO_SUBCTA=4,
#                 FCN_ID_SIEFORE=104,
#                 FTN_DEDUCIBLE=1,
#                 FTN_DISP_PESOS=2301.05,
#                 FTN_DISP_ACCIONES=1300.80,
#                 FCN_ID_PLAZO=24,
#             ),
#             Row(
#                 FTN_NUM_CTA_INVDUAL="NUM_CTA_5",
#                 FCN_ID_TIPO_SUBCTA=5,
#                 FCN_ID_SIEFORE=105,
#                 FTN_DEDUCIBLE=0,
#                 FTN_DISP_PESOS=2401.15,
#                 FTN_DISP_ACCIONES=1400.90,
#                 FCN_ID_PLAZO=1000,  # Sin plazo
#             ),
#             Row(
#                 FTN_NUM_CTA_INVDUAL="NUM_CTA_6",
#                 FCN_ID_TIPO_SUBCTA=6,
#                 FCN_ID_SIEFORE=106,
#                 FTN_DEDUCIBLE=1,
#                 FTN_DISP_PESOS=2501.25,
#                 FTN_DISP_ACCIONES=1500.00,
#                 FCN_ID_PLAZO=36,
#             ),
#             Row(
#                 FTN_NUM_CTA_INVDUAL="NUM_CTA_7",
#                 FCN_ID_TIPO_SUBCTA=7,
#                 FCN_ID_SIEFORE=107,
#                 FTN_DEDUCIBLE=0,
#                 FTN_DISP_PESOS=2601.35,
#                 FTN_DISP_ACCIONES=1600.10,
#                 FCN_ID_PLAZO=1000,  # Sin plazo
#             ),
#             Row(
#                 FTN_NUM_CTA_INVDUAL="NUM_CTA_8",
#                 FCN_ID_TIPO_SUBCTA=8,
#                 FCN_ID_SIEFORE=108,
#                 FTN_DEDUCIBLE=1,
#                 FTN_DISP_PESOS=2701.45,
#                 FTN_DISP_ACCIONES=1700.20,
#                 FCN_ID_PLAZO=48,
#             ),
#             Row(
#                 FTN_NUM_CTA_INVDUAL="NUM_CTA_9",
#                 FCN_ID_TIPO_SUBCTA=9,
#                 FCN_ID_SIEFORE=109,
#                 FTN_DEDUCIBLE=0,
#                 FTN_DISP_PESOS=2801.55,
#                 FTN_DISP_ACCIONES=1800.30,
#                 FCN_ID_PLAZO=1000,  # Sin plazo
#             ),
#             Row(
#                 FTN_NUM_CTA_INVDUAL="NUM_CTA_10",
#                 FCN_ID_TIPO_SUBCTA=10,
#                 FCN_ID_SIEFORE=110,
#                 FTN_DEDUCIBLE=1,
#                 FTN_DISP_PESOS=2901.65,
#                 FTN_DISP_ACCIONES=1900.40,
#                 FCN_ID_PLAZO=60,
#             ),
#         ]

#         # Crear el DataFrame
#         DF_300_TTAFOGRAL_BALANCE_MOVS = spark.createDataFrame(data)

#         # Mostrar el DataFrame
#         display(DF_300_TTAFOGRAL_BALANCE_MOVS)

# COMMAND ----------

# MAGIC %md
# MAGIC ### - hacemos un inner join de los DF anteriores por estas keys:
# MAGIC     - FTN_NUM_CTA_INVDUAL
# MAGIC     - FCN_ID_TIPO_SUBCTA
# MAGIC     - FCN_ID_SIEFORE
# MAGIC     - FTN_DEDUCIBLE

# COMMAND ----------

# Realiza el inner join basado en las claves especificadas
DF_DISPERSION_01 = DF_100_TTSISGRAL_ETL_DISPERSION.join(
    DF_300_TTAFOGRAL_BALANCE_MOVS,
    on=["FTN_NUM_CTA_INVDUAL", "FCN_ID_TIPO_SUBCTA", "FCN_ID_SIEFORE", "FTN_DEDUCIBLE"],
    how="inner"
)

# unpersist DF_300_TTAFOGRAL_BALANCE_MOVS y DF_100_TTSISGRAL_ETL_DISPERSION
DF_300_TTAFOGRAL_BALANCE_MOVS.unpersist
DF_100_TTSISGRAL_ETL_DISPERSION.unpersist
del DF_300_TTAFOGRAL_BALANCE_MOVS, DF_100_TTSISGRAL_ETL_DISPERSION

# insertamos DF_DISPERSION_01 al cache
DF_DISPERSION_01.cache()

if debug:
    # Muestra el DataFrame resultante
    display(DF_DISPERSION_01)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora calculo estas variables en base a los registros de `DF_DISPERSION_01`
# MAGIC - MONTOIMSS = DecimalToDecimal(TOTAL_PESOS / FCN_VALOR_ACCION,"round_inf")
# MAGIC - MONTOISSSTE = DecimalToDecimal(TOTAL_ACCIONES,"round_inf")

# COMMAND ----------

from pyspark.sql.functions import col, expr
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# Función auxiliar para redondear decimal
def DecimalToDecimal(value, round_type):
    if round_type == "round_inf":
        decimals = 2
        factor = F.lit(
            10**decimals
        )  # Convertimos el factor en una constante de PySpark
        return F.floor(value * factor) / factor
    else:
        return value


# Calcula MONTOIMSS utilizando DecimalToDecimal
DF_DISPERSION_01 = DF_DISPERSION_01.withColumn(
    "MONTOIMSS",
    F.round(
        F.col("FTN_DISP_PESOS").cast(DoubleType())
        / F.col("FCN_VALOR_ACCION").cast(DoubleType()),
        10,
    ),
)

# Calcula MONTOISSSTE utilizando DecimalToDecimal
DF_DISPERSION_01 = DF_DISPERSION_01.withColumn(
    "MONTOISSSTE", F.round(F.col("FTN_DISP_ACCIONES"), 6)
)

if debug:
    # Muestra el DataFrame resultante
    display(DF_DISPERSION_01)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aplicamos esta regla al campo FTN_ID_ERROR_VAL (Debemos generar este campo):
# MAGIC ```
# MAGIC IF FTC_FOLIO = '201905070839030073' 
# MAGIC     AND (FTC_FOLIO_REL = '201905081314360765') 
# MAGIC THEN 
# MAGIC     SETNULL()
# MAGIC ELSE
# MAGIC     IF INSTITUTO = 'IMSS' 
# MAGIC     THEN
# MAGIC         IF FTN_DISP_ACCIONES >= MONTOIMSS 
# MAGIC         THEN
# MAGIC             SETNULL()
# MAGIC         ELSE
# MAGIC             SETNULL()
# MAGIC     ELSE
# MAGIC         IF INSTITUTO = 'ISSSTE' 
# MAGIC         THEN
# MAGIC             IF FTN_DISP_ACCIONES >= MONTOISSSTE 
# MAGIC             THEN
# MAGIC                 SETNULL()
# MAGIC             ELSE
# MAGIC                 283
# MAGIC         ELSE 
# MAGIC             SETNULL()
# MAGIC ```
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import when, col, lit

# Aplicamos la regla para generar el campo FTN_ID_ERROR_VAL
DF_DISPERSION_01 = DF_DISPERSION_01.withColumn(
    "FTN_ID_ERROR_VAL",
    when(
        (col("FTC_FOLIO") == '201905070839030073') & (col("FTC_FOLIO_REL") == '201905081314360765'),
        lit(None)  # SETNULL()
    ).otherwise(
        when(
            col("INSTITUTO") == 'IMSS',
            when(col("FTN_DISP_ACCIONES") >= col("MONTOIMSS"), lit(None))  # SETNULL()
            .otherwise(lit(None))  # SETNULL()
        ).when(
            col("INSTITUTO") == 'ISSSTE',
            when(col("FTN_DISP_ACCIONES") >= col("MONTOISSSTE"), lit(None))  # SETNULL()
            .otherwise(lit(283))
        ).otherwise(lit(None))  # SETNULL()
    )
)

if debug:
    # Muestra el DataFrame resultante
    display(DF_DISPERSION_01)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Hacemos un select de solo los datos que ocupamos

# COMMAND ----------

columns_to_select = [
    "FCN_ID_TIPO_SUBCTA",
    "FTC_FOLIO",
    "FCN_ID_SIEFORE",
    "FTF_MONTO_PESOS",
    "FTF_MONTO_ACCIONES",
    "FTN_NUM_CTA_INVDUAL",
    "FTD_FEC_CRE",
    "FTC_USU_CRE",
    "FTD_FEC_ACT",
    "FTC_USU_ACT",
    "FFN_ID_CONCEPTO_MOV",
    "FTC_TABLA_NCI_MOV",
    "FNN_ID_REFERENCIA",
    "FCN_ID_VALOR_ACCION",
    "FTN_ID_MARCA",
    "FCN_VALOR_ACCION",
    "FTC_FOLIO_REL",
    "INSTITUTO",
    "FTN_DEDUCIBLE",
    "FTN_ID_ERROR_VAL",
    "FCN_ID_PLAZO"
]


# COMMAND ----------

DF_DISPERSION_01 = DF_DISPERSION_01.select(*columns_to_select)

if debug:
    display(DF_DISPERSION_01)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Guardamos `DF_DISPERSION_01` en una vista global, llamaremos a la vista `temp_dispersion_mov_01_{global_params['sr_folio']}`

# COMMAND ----------

# Save main_df to a global view named temp_dispersion_01_{global_params['sr_folio']}
view_name = f"temp_dispersion_mov_01_{global_params['SR_FOLIO']}"

spark.sql(f"DROP TABLE IF EXISTS {global_confs['catalog_name']}.{global_confs['schema_name']}.{view_name}")
DF_DISPERSION_01.write.format("delta").mode("overwrite").saveAsTable(
    f"{global_confs['catalog_name']}.{global_confs['schema_name']}.{view_name}"
)

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
DF_DISPERSION_01.unpersist()

# Eliminar DataFrames para liberar memoria
del DF_DISPERSION_01

# Recolector de basura para liberar recursos inmediatamente
import gc
gc.collect()
