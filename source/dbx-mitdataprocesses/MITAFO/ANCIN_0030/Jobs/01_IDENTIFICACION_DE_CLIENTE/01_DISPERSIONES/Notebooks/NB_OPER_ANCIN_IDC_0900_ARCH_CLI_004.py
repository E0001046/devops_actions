# Databricks notebook source
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
# MAGIC import re
# MAGIC
# MAGIC from pyspark.sql.types import (
# MAGIC     StructType,
# MAGIC     StructField,
# MAGIC     StringType,
# MAGIC     DoubleType,
# MAGIC     IntegerType,
# MAGIC )
# MAGIC from pyspark.sql.functions import length, lit
# MAGIC
# MAGIC
# MAGIC def input_values():
# MAGIC     """Retrieve input values from widgets.
# MAGIC     Returns:
# MAGIC         tuple: A tuple containing the input values and a status flag.
# MAGIC     """
# MAGIC     try:
# MAGIC         dbutils.widgets.text("sr_proceso", "")
# MAGIC         dbutils.widgets.text("sr_subproceso", "")
# MAGIC         dbutils.widgets.text("sr_subetapa", "")
# MAGIC         dbutils.widgets.text("sr_origen_arc", "")
# MAGIC         dbutils.widgets.text("sr_dt_org_arc", "")
# MAGIC         dbutils.widgets.text("sr_folio", "")
# MAGIC         dbutils.widgets.text("sr_id_archivo", "")
# MAGIC         dbutils.widgets.text("sr_tipo_layout", "")
# MAGIC         dbutils.widgets.text("sr_instancia_proceso", "")
# MAGIC         dbutils.widgets.text("sr_usuario", "")
# MAGIC         dbutils.widgets.text("sr_etapa", "")
# MAGIC         dbutils.widgets.text("sr_id_snapshot", "")
# MAGIC         dbutils.widgets.text("sr_paso", "")
# MAGIC
# MAGIC         sr_proceso = dbutils.widgets.get("sr_proceso")
# MAGIC         sr_subproceso = dbutils.widgets.get("sr_subproceso")
# MAGIC         sr_subetapa = dbutils.widgets.get("sr_subetapa")
# MAGIC         sr_origen_arc = dbutils.widgets.get("sr_origen_arc")
# MAGIC         sr_dt_org_arc = dbutils.widgets.get("sr_dt_org_arc")
# MAGIC         sr_folio = dbutils.widgets.get("sr_folio")
# MAGIC         sr_id_archivo = dbutils.widgets.get("sr_id_archivo")
# MAGIC         sr_tipo_layout = dbutils.widgets.get("sr_tipo_layout")
# MAGIC         sr_instancia_proceso = dbutils.widgets.get("sr_instancia_proceso")
# MAGIC         sr_usuario = dbutils.widgets.get("sr_usuario")
# MAGIC         sr_etapa = dbutils.widgets.get("sr_etapa")
# MAGIC         sr_id_snapshot = dbutils.widgets.get("sr_id_snapshot")
# MAGIC         sr_paso = dbutils.widgets.get("sr_paso")
# MAGIC
# MAGIC         if any(
# MAGIC             len(str(value).strip()) == 0
# MAGIC             for value in [
# MAGIC                 sr_proceso,
# MAGIC                 sr_subproceso,
# MAGIC                 sr_subetapa,
# MAGIC                 sr_origen_arc,
# MAGIC                 sr_dt_org_arc,
# MAGIC                 sr_folio,
# MAGIC                 sr_id_archivo,
# MAGIC                 sr_tipo_layout,
# MAGIC                 sr_instancia_proceso,
# MAGIC                 sr_usuario,
# MAGIC                 sr_etapa,
# MAGIC                 sr_id_snapshot,
# MAGIC                 sr_paso,
# MAGIC             ]
# MAGIC         ):
# MAGIC             logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC             logger.error("Some of the input values are empty or null")
# MAGIC             return "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: %s", str(e))
# MAGIC         return "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0"
# MAGIC     return (
# MAGIC         sr_proceso,
# MAGIC         sr_subproceso,
# MAGIC         sr_subetapa,
# MAGIC         sr_origen_arc,
# MAGIC         sr_dt_org_arc,
# MAGIC         sr_folio,
# MAGIC         sr_id_archivo,
# MAGIC         sr_tipo_layout,
# MAGIC         sr_instancia_proceso,
# MAGIC         sr_usuario,
# MAGIC         sr_etapa,
# MAGIC         sr_id_snapshot,
# MAGIC         sr_paso,
# MAGIC         "1",
# MAGIC     )
# MAGIC
# MAGIC
# MAGIC def conf_process_values(config_file, process_name):
# MAGIC     """Retrieve process configuration values from a config file.
# MAGIC     Args:
# MAGIC         config_file (str): Path to the configuration file.
# MAGIC         process_name (str): Name of the process.
# MAGIC     Returns:
# MAGIC         tuple: A tuple containing the configuration values and a status flag.
# MAGIC     """
# MAGIC     try:
# MAGIC         config = configparser.ConfigParser()
# MAGIC         config.read(config_file)
# MAGIC         # Subprocess configurations
# MAGIC         #
# MAGIC         sql_conf_file = config.get(process_name, "sql_conf_file")
# MAGIC         temp_view_schema = config.get(process_name, "global_temp_view_schema_001")
# MAGIC         global_temp_view_009 = config.get(process_name, "global_temp_view_009")
# MAGIC         global_temp_view_010 = config.get(process_name, "global_temp_view_010")
# MAGIC         global_temp_view_011 = config.get(process_name, "global_temp_view_011")
# MAGIC         #
# MAGIC         external_location = config.get(process_name, "external_location")
# MAGIC         out_repo_path = config.get(process_name, "out_repo_path")
# MAGIC         output_file_name_001 = config.get(process_name, "output_file_name_001")
# MAGIC         sep = config.get(process_name, "sep")
# MAGIC         header = config.get(process_name, "header")
# MAGIC         # Unity
# MAGIC         debug = config.get(process_name, "debug")
# MAGIC         debug = debug.lower() == "true"
# MAGIC         catalog_name = config.get(process_name, "catalog_name")
# MAGIC         schema_name = config.get(process_name, "schema_name")
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC         return (
# MAGIC             "0",
# MAGIC             "0",
# MAGIC             "0",
# MAGIC             "0",
# MAGIC             "0",
# MAGIC             "0",
# MAGIC             "0",
# MAGIC             "0",
# MAGIC             "0",
# MAGIC             "0",
# MAGIC         )
# MAGIC     return (
# MAGIC         sql_conf_file,
# MAGIC         temp_view_schema,
# MAGIC         global_temp_view_009,
# MAGIC         global_temp_view_010,
# MAGIC         global_temp_view_011,
# MAGIC         external_location,
# MAGIC         out_repo_path,
# MAGIC         output_file_name_001,
# MAGIC         sep,
# MAGIC         header,
# MAGIC         # unity
# MAGIC         debug,
# MAGIC         catalog_name,
# MAGIC         schema_name,
# MAGIC         "1",
# MAGIC     )
# MAGIC
# MAGIC
# MAGIC def fix_created_file(file_name):
# MAGIC     try:
# MAGIC         full_file_name_aux = full_file_name + "_TEMP"
# MAGIC         dbutils.fs.rm(full_file_name_aux, recurse=True)
# MAGIC         file_name_tmp = dbutils.fs.ls(full_file_name)
# MAGIC         file_name_new = list(filter(lambda x: x[0].endswith("csv"), file_name_tmp))
# MAGIC         dbutils.fs.cp(file_name_new[0][0], full_file_name_aux)
# MAGIC         dbutils.fs.rm(full_file_name, recurse=True)
# MAGIC         dbutils.fs.cp(full_file_name_aux, full_file_name)
# MAGIC         dbutils.fs.rm(full_file_name_aux, recurse=True)
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: " + str(inspect.stack()[0][3]))
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC         return "0"
# MAGIC     return "1"
# MAGIC
# MAGIC
# MAGIC if __name__ == "__main__":
# MAGIC     logging.basicConfig()
# MAGIC     logger = logging.getLogger(__name__)
# MAGIC     logger.setLevel(logging.DEBUG)
# MAGIC     notebook_name = (
# MAGIC         dbutils.notebook.entry_point.getDbutils()
# MAGIC         .notebook()
# MAGIC         .getContext()
# MAGIC         .notebookPath()
# MAGIC         .get()
# MAGIC     )
# MAGIC     message = "NB Error: " + notebook_name
# MAGIC     source = "ETL"
# MAGIC     root_repo = "/Workspace/Shared/MITAFO"
# MAGIC
# MAGIC     try:
# MAGIC         sys.path.append(root_repo + "/" + "CGRLS_0010/Notebooks")
# MAGIC         from NB_GRLS_DML_FUNCTIONS import *
# MAGIC         from NB_GRLS_SIMPLE_FUNCTIONS import *
# MAGIC     except Exception as e:
# MAGIC         logger.error("Error at the beggining of the process")
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC
# MAGIC     config_file = root_repo + "/" + "CGRLS_0010/Conf/CF_GRLS_PROCESS.py.properties"
# MAGIC     config_conn_file = root_repo + "/" + "CGRLS_0010/Conf/CF_GRLS_CONN.py.properties"
# MAGIC     config_process_file = (
# MAGIC         root_repo
# MAGIC         + "/"
# MAGIC         + "ANCIN_0030/Jobs/01_IDENTIFICACION_DE_CLIENTE/01_DISPERSIONES/Conf/CF_PART_PROC_UNITY.py.properties"
# MAGIC     )
# MAGIC
# MAGIC     (
# MAGIC         sr_proceso,
# MAGIC         sr_subproceso,
# MAGIC         sr_subetapa,
# MAGIC         sr_origen_arc,
# MAGIC         sr_dt_org_arc,
# MAGIC         sr_folio,
# MAGIC         sr_id_archivo,
# MAGIC         sr_tipo_layout,
# MAGIC         sr_instancia_proceso,
# MAGIC         sr_usuario,
# MAGIC         sr_etapa,
# MAGIC         sr_id_snapshot,
# MAGIC         sr_paso,
# MAGIC         failed_task,
# MAGIC     ) = input_values()
# MAGIC     if failed_task == "0":
# MAGIC         logger.error("Please review log messages")
# MAGIC         # notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("An error ocurred, check out log messages")
# MAGIC
# MAGIC     input_parameters = dbutils.widgets.getAll().items()
# MAGIC
# MAGIC     process_name = "root"
# MAGIC     webhook_url, channel, failed_task = conf_init_values(
# MAGIC         config_file, process_name, "IDC"
# MAGIC     )
# MAGIC     if failed_task == "0":
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     process_name = "root"
# MAGIC     (
# MAGIC         sql_conf_file,
# MAGIC         temp_view_schema,
# MAGIC         global_temp_view_009,
# MAGIC         global_temp_view_010,
# MAGIC         global_temp_view_011,
# MAGIC         external_location,
# MAGIC         out_repo_path,
# MAGIC         output_file_name_001,
# MAGIC         sep,
# MAGIC         header,
# MAGIC         # unity
# MAGIC         debug,
# MAGIC         catalog_name,
# MAGIC         schema_name,
# MAGIC         failed_task,
# MAGIC     ) = conf_process_values(config_process_file, process_name)
# MAGIC     if failed_task == "0":
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     conn_name_ora = "jdbc_oracle"
# MAGIC     (
# MAGIC         conn_options,
# MAGIC         conn_aditional_options,
# MAGIC         conn_user,
# MAGIC         conn_key,
# MAGIC         conn_url,
# MAGIC         scope,
# MAGIC         failed_task,
# MAGIC     ) = conf_conn_values(config_conn_file, conn_name_ora)
# MAGIC     if failed_task == "0":
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     sql_conf_file = (
# MAGIC         root_repo
# MAGIC         + "/ANCIN_0030/Jobs/01_IDENTIFICACION_DE_CLIENTE/01_DISPERSIONES/JSON/"
# MAGIC         + sql_conf_file
# MAGIC     )

# COMMAND ----------

with open(sql_conf_file) as f:
    file_config_sql = json.load(f)

conf_values = [ (fields['step_id'], '\n'.join(fields['value'])) for line, value in file_config_sql.items() if line == 'steps' for fields in value ]

# COMMAND ----------

query_statement = '028'

global_temp_view_009 = f"{catalog_name}.{schema_name}.{global_temp_view_009}_{sr_id_archivo}"
global_temp_view_010 = f"{catalog_name}.{schema_name}.{global_temp_view_010}_{sr_id_archivo}"
global_temp_view_011 = f"{catalog_name}.{schema_name}.{global_temp_view_011}_{sr_id_archivo}"

params = [global_temp_view_009, global_temp_view_010, global_temp_view_011]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

try:
    df = spark.sql(statement)
    if debug:
        print(statement)
        display(df)
except Exception as e:
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")
 
#spark.sql("select * from TEMP_DOIMSS_MCV_MONTOS_CLIENTE").show(10)
#display(df)

# COMMAND ----------

temp_view = 'TEMP_ARCHIVO_CLIENTES_004' + '_' + sr_id_archivo

#spark.catalog.dropGlobalTempView(temp_view)

try:
    # df.createOrReplaceTempView(temp_view)
    df.write.format("delta").mode("overwrite").saveAsTable(
        f"{catalog_name}.{schema_name}.{temp_view}"
    )
except Exception as e:
    logger.error("Function: %s", inspect.stack()[0][3])
    logger.error("An error was raised: " + str(e))
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    
del df
    #raise Exception("An error raised")

# COMMAND ----------

query_statement = "029"

global_temp_view_aux = f"{catalog_name}.{schema_name}.TEMP_SIEFORE01_{sr_id_archivo}"
temp_view = f"{catalog_name}.{schema_name}.TEMP_ARCHIVO_CLIENTES_004_{sr_id_archivo}"

params = [temp_view, global_temp_view_aux]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == "0":
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

try:
    df = spark.sql(statement)
    if debug:
        display(df)
except Exception as e:
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

# COMMAND ----------

temp_view = 'TEMP_ARCHIVO_CLIENTES_005' + '_' + sr_id_archivo

#spark.catalog.dropGlobalTempView(temp_view)

try:
    df.write.format("delta").mode("overwrite").saveAsTable(
        f"{catalog_name}.{schema_name}.{temp_view}"
    )
    # df.createOrReplaceTempView(temp_view)
except Exception as e:
    logger.error("Function: %s", inspect.stack()[0][3])
    logger.error("An error was raised: " + str(e))
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)

# COMMAND ----------

query_statement = '030'
temp_view = f"{catalog_name}.{schema_name}.TEMP_ARCHIVO_CLIENTES_005_{sr_id_archivo}"

params = [temp_view]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

try:
    df = spark.sql(statement)
    if debug:
        display(df)
except Exception as e:
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")


# COMMAND ----------

temp_view = 'TEMP_ARCHIVO_CLIENTES_006' + '_' + sr_id_archivo

#spark.catalog.dropGlobalTempView(temp_view)

try:
    df.write.format("delta").mode("overwrite").saveAsTable(
        f"{catalog_name}.{schema_name}.{temp_view}"
    )
    # df.createOrReplaceTempView(temp_view)
except Exception as e:
    logger.error("Function: %s", inspect.stack()[0][3])
    logger.error("An error was raised: " + str(e))
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)

# COMMAND ----------

#Mismo que el 30
query_statement = '031'
temp_view = f"{catalog_name}.{schema_name}.TEMP_ARCHIVO_CLIENTES_006_{sr_id_archivo}"

params = [temp_view]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

try:
    df = spark.sql(statement)
    if debug:
        display(df)
except Exception as e:
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

# COMMAND ----------

#Crea la vista para realizar el pivote
temp_view = 'TEMP_ARCHIVO_CLIENTES_007' + '_' + sr_id_archivo

try:
    df.write.format("delta").mode("overwrite").saveAsTable(
        f"{catalog_name}.{schema_name}.{temp_view}"
    )
    # df.createOrReplaceTempView(temp_view)
except Exception as e:
    logger.error("Function: %s", inspect.stack()[0][3])
    logger.error("An error was raised: " + str(e))
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)

# COMMAND ----------

# DBTITLE 1,Original
# # Realiza el pivote
# list_pivot = spark.sql(
#     "SELECT DISTINCT FCN_ID_GRUPO FROM " + temp_view
# ).collect()

# j = []
# for i in list_pivot:
#     j.append(str(i[0]))

# list_pivot = ",".join(j)

# try:
#     df = spark.sql(
#         "SELECT * FROM (SELECT * FROM "
#         + temp_view
#         + ") PIVOT (COUNT(FCN_ID_GRUPO) FOR FCN_ID_GRUPO IN ("
#         + list_pivot
#         + "))"
#     )
# except Exception as e:
#     logger.error("No value %s found", statement)
#     logger.error("Please review log messages")
#     notification_raised(webhook_url, -1, message, source, input_parameters)
#     raise Exception("Process ends")

# COMMAND ----------

# DBTITLE 1,Test
temp_view = f"{catalog_name}.{schema_name}.TEMP_ARCHIVO_CLIENTES_007_{sr_id_archivo}"
# Realiza el pivote
list_pivot = spark.sql(f"SELECT DISTINCT FCN_ID_GRUPO FROM {temp_view}").collect()

j = []
for i in list_pivot:
    j.append(f"'{i[0]}'")

list_pivot = ",".join(j)

if not list_pivot: # si list_pivot esta vacio
    list_pivot = "Null"

try:
    statement = f"""
        SELECT * FROM (SELECT * FROM {temp_view}) 
        PIVOT (COUNT(FCN_ID_GRUPO) FOR FCN_ID_GRUPO IN ({list_pivot}))
    """
    df = spark.sql(statement)
    if debug:
        display(df)
except Exception as e:
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

# COMMAND ----------

j = []
# for i in list_pivot.split(","):
#     j.append(re.sub("$", "`, '0000')", re.sub("^", "NVL(`", str(i))))

for i in list_pivot.split(","):
    j.append(re.sub("$", "', '0000')", re.sub("^", "NVL('", str(i))))

list_columns = ",".join(j)


statement = (
    "SELECT FTN_NSS ,FTC_CURP ,FTC_NOMBRE ,FTC_NUM_CUENTA ,FTC_ID_DIAGNOSTICO ,FTD_FECHA_CERTIFICACION ,FTC_ID_SUBP_NO_VIG ,FTC_DIAGNOSTICO ,FTN_NUM_CTA_INVDUAL ,FCN_ID_TIPO_SUBCTA ,FCN_ID_GRUPO ,FCN_ID_SIEFORE ,MARCA, CONCAT_WS(',', "
    + list_columns
    + ") AS GROUP_VALUES FROM "
    + temp_view
)

# statement = (
#     "SELECT FTN_NSS ,FTC_CURP ,FTC_NOMBRE ,FTC_NUM_CUENTA ,FTC_ID_DIAGNOSTICO ,FTD_FECHA_CERTIFICACION ,FTC_ID_SUBP_NO_VIG ,FTC_DIAGNOSTICO ,FTN_NUM_CTA_INVDUAL ,FCN_ID_TIPO_SUBCTA ,FCN_ID_GRUPO ,FCN_ID_SIEFORE ,MARCA FROM "
#     + temp_view
# )

try:
    df = spark.sql(statement)
    if debug:
        display(df)
except Exception as e:
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

# COMMAND ----------

temp_view = 'TEMP_ARCHIVO_CLIENTES_008' + '_' + sr_id_archivo
try:
    df.write.format("delta").mode("overwrite").saveAsTable(
        f"{catalog_name}.{schema_name}.{temp_view}"
    )
    # df.createOrReplaceTempView(temp_view)
except Exception as e:
    logger.error("Function: %s", inspect.stack()[0][3])
    logger.error("An error was raised: " + str(e))
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)

# COMMAND ----------

query_statement = '033'

params = [f"{catalog_name}.{schema_name}.TEMP_ARCHIVO_CLIENTES_006" + "_" + sr_id_archivo, f"{catalog_name}.{schema_name}.TEMP_ARCHIVO_CLIENTES_008" + "_" + sr_id_archivo]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

try:
    df = spark.sql(statement)
    if debug:
        display(df)
    df.cache()
except Exception as e:
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

# COMMAND ----------

query_statement = f"""
SELECT
val.FTN_NSS
,val.FTC_CURP
,val.FTC_NOMBRE_CTE
,val.FTN_NUM_CTA_INVDUAL
,tcc.FCC_VALOR AS DIAGNOSTICO
,val.FTC_ID_SUBP_NO_VIG
,TO_CHAR(val.FTD_FECHA_CERTIFICACION, 'DD/MM/YYYY') AS FTD_FECHA_CERTIFICACION
FROM CIERREN_ETL.TTSISGRAL_ETL_VAL_IDENT_CTE val
LEFT JOIN CIERREN.TCCRXGRAL_CAT_CATALOGO tcc
ON val.FTC_ID_DIAGNOSTICO = tcc.FCN_ID_CAT_CATALOGO
WHERE val.FTC_FOLIO IN ('{sr_folio}') AND val.FTN_ESTATUS_DIAG = 0
"""

# COMMAND ----------

df, failed_task = query_table(conn_name_ora, spark, query_statement,  conn_options, conn_user, conn_key)

if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")

if debug:
    display(df)


# COMMAND ----------

full_file_name =  external_location + out_repo_path + '/' + sr_folio + '_' + output_file_name_001

file_name = full_file_name + '_TEMP'

header = True

try:
    df.write.format("csv").mode('overwrite').option("header", header).save(full_file_name + '_TEMP')
    dataframe = spark.read.option('header', header).csv(full_file_name + '_TEMP')
    dataframe.coalesce(1).write.format('csv').mode('overwrite').option('header', True).save(full_file_name)
except Exception as e:
    logger.error("An error was raised: " + str(e))
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

# COMMAND ----------

failed_task = fix_created_file(full_file_name)

lines = dbutils.fs.head(full_file_name)

if (lines.count('\n') == 1 and header) or (lines.count('\n') == 0):
    dbutils.fs.rm(full_file_name, recurse = True)

if failed_task == '0':
        logger.error("Please review log messages")
        notification_raised(webhook_url, -1, message, source, input_parameters)
        raise Exception("Process ends")

# COMMAND ----------

notification_raised(webhook_url, 0, "DONE", source, input_parameters)

# COMMAND ----------

# Liberar la caché del DataFrame
df.unpersist()

# Si ya no necesitas df en lo absoluto, puedes eliminarlo
del df

# Forzar la recolección de basura para liberar memoria
import gc
gc.collect()
