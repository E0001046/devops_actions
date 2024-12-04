# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC %reload_ext autoreload
# MAGIC import configparser
# MAGIC import inspect
# MAGIC import json
# MAGIC import logging
# MAGIC import sys
# MAGIC
# MAGIC logger = logging.getLogger(__name__)
# MAGIC
# MAGIC
# MAGIC logging.basicConfig()
# MAGIC logger = logging.getLogger(__name__)
# MAGIC logger.setLevel(logging.DEBUG)
# MAGIC
# MAGIC ROOT_REPO = "/Workspace/Shared/MITAFO"
# MAGIC sys.path.append(ROOT_REPO + "/" + "CGRLS_0010/Notebooks")
# MAGIC
# MAGIC # TODO: CAMBIAR A CLASES E IMPORTAR OBJETO DE LA CLASE PARA EVITAR '*'
# MAGIC from NB_GRLS_DML_FUNCTIONS import *
# MAGIC from NB_GRLS_SIMPLE_FUNCTIONS import *
# MAGIC
# MAGIC
# MAGIC def input_values() -> tuple:
# MAGIC     """Obtiene los valores de los widgets de entrada.
# MAGIC
# MAGIC     Respuesta:
# MAGIC     ----------
# MAGIC         tuple: Una tupla que contiene los valores de los widgets de entrada y
# MAGIC         una bandera de estado.
# MAGIC     """
# MAGIC
# MAGIC     # Definir claves y valores predeterminados para los widgets
# MAGIC     widget_defaults = {
# MAGIC         "SR_FTC_FOLIO": "202311211301042007",
# MAGIC         "SR_FOLIO_REL": "302311211301042008",
# MAGIC         "SR_FEC_ACC": "2024-07-30",
# MAGIC         "SR_TIPO_MOV": "2",
# MAGIC         "SR_FTD_FEH_LIQUIDACION": "2024-07-30",
# MAGIC         "SR_FCC_USU_CRE": "DATABRICKS",
# MAGIC         "SR_FACTOR": "1",
# MAGIC         "SR_FLC_USU_REG": "DATABRICKS",
# MAGIC         "SR_ETAPA": "1",
# MAGIC         "SR_CX_CRE_ESQUEMA": "CIERREN_ETL",
# MAGIC         "SR_TL_CRE_DISPERSION": "TTSISGRAL_ETL_DISPERSION",
# MAGIC     }
# MAGIC
# MAGIC     # Crear widgets con valores predeterminados
# MAGIC     for key, default_value in widget_defaults.items():
# MAGIC         dbutils.widgets.text(key, default_value)
# MAGIC
# MAGIC     try:
# MAGIC         # Recuperar los valores de los widgets
# MAGIC         values = {key: dbutils.widgets.get(key).strip() for key in widget_defaults}
# MAGIC
# MAGIC         # Verificar si alguno de los valores está vacío
# MAGIC         if any(not value for value in values.values()):
# MAGIC             logger.error("Function: input_values")
# MAGIC             logger.error("Some of the input values are empty or null")
# MAGIC             return tuple("0" for _ in widget_defaults) + ("0",)
# MAGIC
# MAGIC         # Todos los valores son válidos
# MAGIC         return tuple(values.values()) + ("1",)
# MAGIC
# MAGIC     except (ValueError, IOError) as error:
# MAGIC         logger.error("Function: input_values")
# MAGIC         logger.error("An error was raised: %s", str(error))
# MAGIC         return tuple("0" for _ in widget_defaults) + ("0",)
# MAGIC
# MAGIC
# MAGIC def conf_process_values(arg_config_file: str, arg_process_name: str) -> tuple:
# MAGIC     """Obtiene los valores de configuración del proceso.
# MAGIC
# MAGIC     Parámetros:
# MAGIC     -----------
# MAGIC         arg_CONFIG_FILE (str): Ruta del archivo de configuración.
# MAGIC         arf_PROCESS_NAME (str): Nombre del proceso.
# MAGIC
# MAGIC     Respuesta:
# MAGIC     ----------
# MAGIC         tuple: Una tupla que contiene los valores de configuración del proceso
# MAGIC         y una bandera de estado.
# MAGIC     """
# MAGIC     keys = [
# MAGIC         "sql_conf_file",
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
# MAGIC         "err_repo_path",
# MAGIC         "output_file_name_001",
# MAGIC         "sep",
# MAGIC         "header",
# MAGIC         "external_location",
# MAGIC     ]
# MAGIC
# MAGIC     try:
# MAGIC         config = configparser.ConfigParser()
# MAGIC         config.read(arg_config_file)
# MAGIC
# MAGIC         # Crear un diccionario con los valores obtenidos
# MAGIC         result = {key: config.get(arg_process_name, key) for key in keys}
# MAGIC
# MAGIC         # Añadir la bandera de estado al diccionario
# MAGIC         result["status"] = "1"
# MAGIC
# MAGIC     except (ValueError, IOError) as error:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: %s", str(error))
# MAGIC         # Devolver un diccionario con valores por defecto y estado de error
# MAGIC         result = {key: "0" for key in keys}
# MAGIC         result["status"] = "0"
# MAGIC
# MAGIC     return tuple(result.values())
# MAGIC
# MAGIC
# MAGIC def fix_created_file(file_name):
# MAGIC     try:
# MAGIC         file_name_tmp = dbutils.fs.ls(file_name + "_TEMP")
# MAGIC         file_name_new = list(filter(lambda x: x[0].endswith("csv"), file_name_tmp))
# MAGIC         dbutils.fs.mv(file_name_new[0][0], file_name)
# MAGIC         dbutils.fs.rm(file_name + "_TEMP", recurse=True)
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: " + str(inspect.stack()[0][3]))
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC         return "0"
# MAGIC     return "1"
# MAGIC
# MAGIC
# MAGIC if __name__ == "__main__":
# MAGIC
# MAGIC     notebook_name = (
# MAGIC         dbutils.notebook.entry_point.getDbutils()
# MAGIC         .notebook()
# MAGIC         .getContext()
# MAGIC         .notebookPath()
# MAGIC         .get()
# MAGIC     )
# MAGIC
# MAGIC     MESSAGE = "NB Error: " + notebook_name
# MAGIC     SOURCE = "ETL"
# MAGIC     CONFIG_FILE = ROOT_REPO + "/" + "CGRLS_0010/Conf/CF_GRLS_PROCESS.py.properties"
# MAGIC     CONFIG_CONN_FILE = ROOT_REPO + "/" + "CGRLS_0010/Conf/CF_GRLS_CONN.py.properties"
# MAGIC
# MAGIC     # TODO: CONTROLAR POR VARIABLE DE ENTORNO
# MAGIC     CONFIG_PROCESS_FILE = (
# MAGIC         ROOT_REPO
# MAGIC         + "/"
# MAGIC         + "ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/DISPERSIONES/Conf/CF_PART_PROC.py.properties"
# MAGIC     )
# MAGIC
# MAGIC     (
# MAGIC         SR_FTC_FOLIO,
# MAGIC         SR_FOLIO_REL,
# MAGIC         SR_FEC_ACC,
# MAGIC         SR_TIPO_MOV,
# MAGIC         SR_FTD_FEH_LIQUIDACION,
# MAGIC         SR_FCC_USU_CRE,
# MAGIC         SR_FACTOR,
# MAGIC         SR_FLC_USU_REG,
# MAGIC         SR_ETAPA,
# MAGIC         SR_CX_CRE_ESQUEMA,
# MAGIC         SR_TL_CRE_DISPERSION,
# MAGIC         failed_task,
# MAGIC     ) = input_values()
# MAGIC
# MAGIC     if failed_task == "0":
# MAGIC         logger.error("Please review log MESSAGEs")
# MAGIC         # notification_raised(webhook_url, -1, MESSAGE, SOURCE, input_parameters)
# MAGIC         raise ValueError("An error ocurred, check out log MESSAGEs")
# MAGIC
# MAGIC     input_parameters = dbutils.widgets.getAll().items()
# MAGIC
# MAGIC     PROCESS_NAME = "root"
# MAGIC     webhook_url, channel, failed_task = conf_init_values(
# MAGIC         CONFIG_FILE,
# MAGIC         PROCESS_NAME,
# MAGIC         "TEMP_PROCESS",
# MAGIC     )
# MAGIC     if failed_task == "0":
# MAGIC         logger.error("Please review log MESSAGEs")
# MAGIC         notification_raised(webhook_url, -1, MESSAGE, SOURCE, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     PROCESS_NAME = "root"
# MAGIC     (
# MAGIC         sql_conf_file,
# MAGIC         conn_schema_001,
# MAGIC         conn_schema_002,
# MAGIC         table_001,
# MAGIC         table_002,
# MAGIC         table_003,
# MAGIC         table_004,
# MAGIC         table_005,
# MAGIC         table_006,
# MAGIC         table_007,
# MAGIC         table_008,
# MAGIC         table_009,
# MAGIC         table_010,
# MAGIC         table_011,
# MAGIC         err_repo_path,
# MAGIC         output_file_name_001,
# MAGIC         sep,
# MAGIC         header,
# MAGIC         external_location,
# MAGIC         failed_task,
# MAGIC     ) = conf_process_values(CONFIG_PROCESS_FILE, PROCESS_NAME)
# MAGIC     if failed_task == "0":
# MAGIC         logger.error("Please review log MESSAGEs")
# MAGIC         notification_raised(webhook_url, -1, MESSAGE, SOURCE, input_parameters)
# MAGIC         raise ValueError("Process ends")
# MAGIC
# MAGIC     CONN_NAME_ORA = "jdbc_oracle"
# MAGIC     CONN_OPTIONS, CONN_ADDITIONAL_OPTIONS, CON_USER, CONN_KEY, failed_task = (
# MAGIC         conf_conn_values(CONFIG_CONN_FILE, CONN_NAME_ORA)
# MAGIC     )
# MAGIC     if failed_task == "0":
# MAGIC         logger.error("Please review log MESSAGEs")
# MAGIC         notification_raised(webhook_url, -1, MESSAGE, SOURCE, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     # TODO: CONTROLAR POR VARIABLE DE ENTORNO
# MAGIC     sql_conf_file = (
# MAGIC         ROOT_REPO
# MAGIC         + "/"
# MAGIC         + "ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/DISPERSIONES/JSON/"
# MAGIC         + sql_conf_file
# MAGIC     )
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

table_name_001 = conn_schema_001 + "." + table_007

query_statement = "017"

params = [
    table_name_001,
    SR_FTC_FOLIO,
]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == "0":
    logger.error("No value %s found", statement)
    logger.error("Please review log MESSAGEs")
    notification_raised(webhook_url, -1, MESSAGE, SOURCE, input_parameters)
    raise Exception("Process ends")

# COMMAND ----------

df_output, failed_task = query_table(
    CONN_NAME_ORA, spark, statement, CONN_OPTIONS, CON_USER, CONN_KEY
)

# COMMAND ----------

display(df_output)

# COMMAND ----------

# MAGIC %md
# MAGIC # NOTA:
# MAGIC ### del DF anterior se debe generar un archivo .csv
# MAGIC

# COMMAND ----------

# Solo si se requiere validar el archivo generado
# def create_file_on_unity_catalog(df, file_name, sep, header, unity_catalog_path):
#     try:
#         # Crear el archivo directamente en Unity Catalog
#         df.repartition(1).write.mode("overwrite").option("header", header).option(
#             "sep", sep
#         ).csv(unity_catalog_path)
#     except Exception as e:
#         logger.error("Function: " + str(inspect.stack()[0][3]))
#         logger.error("Trying to create file: " + unity_catalog_path)
#         logger.error("Message: " + str(e))
#         return "0"
#     return "1"


# COMMAND ----------

# Contar registros
total_records = df_output.count()

# Crear nombre de archivo completo
full_file_name = (
    external_location + err_repo_path + "/" + SR_FTC_FOLIO + "_" + output_file_name_001
)

# Crear archivo temporal
failed_task = create_file(df_output, full_file_name, sep, header)

if failed_task == "0":
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, MESSAGE, SOURCE, input_parameters)
    raise Exception("An error was raised during file creation")

# Ajustar archivo creado (mover y renombrar)
failed_task = fix_created_file(full_file_name)

if failed_task == "0":
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, MESSAGE, SOURCE, input_parameters)
    raise Exception("An error was raised during file fix")

# ## Crear nombre de archivo completo para Unity Catalog
# unity_catalog_path = (
#     "/Volumes/dbx_mit_dev_1udbvf_workspace/default/jp_pancin_mov_0060_ext_arch/"
#     + SR_FTC_FOLIO
#     + "_"
#     + output_file_name_001
# )

# # Crear archivo directamente en Unity Catalog
# failed_task = create_file_on_unity_catalog(
#     df_output, full_file_name, sep, header, unity_catalog_path
# )

# if failed_task == "0":
#     logger.error("Please review log messages")
#     notification_raised(webhook_url, -1, MESSAGE, SOURCE, input_parameters)
#     raise Exception("An error was raised during file creation")

# logger.info(f"File successfully written to Unity Catalog at {unity_catalog_path}")
