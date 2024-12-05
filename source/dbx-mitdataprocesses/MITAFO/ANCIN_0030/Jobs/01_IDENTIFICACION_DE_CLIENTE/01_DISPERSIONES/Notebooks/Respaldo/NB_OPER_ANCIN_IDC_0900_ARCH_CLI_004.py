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
# MAGIC from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
# MAGIC from pyspark.sql.functions import length, lit
# MAGIC
# MAGIC def input_values():
# MAGIC     """ Retrieve input values from widgets.
# MAGIC     Returns:
# MAGIC         tuple: A tuple containing the input values and a status flag.
# MAGIC     """
# MAGIC     try:
# MAGIC         dbutils.widgets.text('sr_proceso', '')
# MAGIC         dbutils.widgets.text('sr_subproceso', '')
# MAGIC         dbutils.widgets.text('sr_subetapa', '')
# MAGIC         dbutils.widgets.text('sr_origen_arc', '')
# MAGIC         dbutils.widgets.text('sr_dt_org_arc', '')
# MAGIC         dbutils.widgets.text('sr_folio', '')
# MAGIC         dbutils.widgets.text('sr_id_archivo', '')
# MAGIC         dbutils.widgets.text('sr_tipo_layout', '')
# MAGIC         dbutils.widgets.text('sr_instancia_proceso', '')
# MAGIC         dbutils.widgets.text('sr_usuario', '')
# MAGIC         dbutils.widgets.text('sr_etapa', '')
# MAGIC         dbutils.widgets.text('sr_id_snapshot', '')
# MAGIC         dbutils.widgets.text('sr_paso', '')
# MAGIC
# MAGIC         sr_proceso = dbutils.widgets.get('sr_proceso')
# MAGIC         sr_subproceso = dbutils.widgets.get('sr_subproceso')
# MAGIC         sr_subetapa = dbutils.widgets.get('sr_subetapa')
# MAGIC         sr_origen_arc = dbutils.widgets.get('sr_origen_arc')
# MAGIC         sr_dt_org_arc = dbutils.widgets.get('sr_dt_org_arc')
# MAGIC         sr_folio = dbutils.widgets.get('sr_folio')
# MAGIC         sr_id_archivo = dbutils.widgets.get('sr_id_archivo')
# MAGIC         sr_tipo_layout = dbutils.widgets.get('sr_tipo_layout')
# MAGIC         sr_instancia_proceso = dbutils.widgets.get('sr_instancia_proceso')
# MAGIC         sr_usuario = dbutils.widgets.get('sr_usuario')
# MAGIC         sr_etapa = dbutils.widgets.get('sr_etapa')
# MAGIC         sr_id_snapshot = dbutils.widgets.get('sr_id_snapshot')
# MAGIC         sr_paso = dbutils.widgets.get('sr_paso')
# MAGIC
# MAGIC         if any(len(str(value).strip()) == 0 for value in [sr_proceso, sr_subproceso, sr_subetapa, sr_origen_arc, sr_dt_org_arc, sr_folio, sr_id_archivo, sr_tipo_layout, sr_instancia_proceso, sr_usuario, sr_etapa, sr_id_snapshot, sr_paso]):
# MAGIC             logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC             logger.error("Some of the input values are empty or null")
# MAGIC             return '0','0','0','0','0','0','0','0','0','0','0','0','0','0'
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: %s", str(e))
# MAGIC         return '0','0','0','0','0','0','0','0','0','0','0','0','0','0'
# MAGIC     return sr_proceso, sr_subproceso, sr_subetapa, sr_origen_arc, sr_dt_org_arc, sr_folio, sr_id_archivo, sr_tipo_layout, sr_instancia_proceso, sr_usuario, sr_etapa, sr_id_snapshot, sr_paso, '1'
# MAGIC
# MAGIC
# MAGIC
# MAGIC def conf_process_values(config_file, process_name):
# MAGIC     """ Retrieve process configuration values from a config file.
# MAGIC     Args:
# MAGIC         config_file (str): Path to the configuration file.
# MAGIC         process_name (str): Name of the process.
# MAGIC     Returns:
# MAGIC         tuple: A tuple containing the configuration values and a status flag.
# MAGIC     """
# MAGIC     try:
# MAGIC         config = configparser.ConfigParser()
# MAGIC         config.read(config_file)
# MAGIC         #Subprocess configurations
# MAGIC         #
# MAGIC         sql_conf_file = config.get(process_name, 'sql_conf_file')
# MAGIC         temp_view_schema = config.get(process_name, 'global_temp_view_schema_001')
# MAGIC         global_temp_view_009 = config.get(process_name, 'global_temp_view_009')
# MAGIC         global_temp_view_010 = config.get(process_name, 'global_temp_view_010')
# MAGIC         global_temp_view_011 = config.get(process_name, 'global_temp_view_011')
# MAGIC         #
# MAGIC         external_location = config.get(process_name, 'external_location')
# MAGIC         out_repo_path = config.get(process_name, 'out_repo_path')
# MAGIC         output_file_name_001 = config.get(process_name, 'output_file_name_001')
# MAGIC         sep = config.get(process_name, 'sep')
# MAGIC         header = config.get(process_name, 'header')
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC         return '0','0','0','0','0','0','0'
# MAGIC     return sql_conf_file, temp_view_schema, global_temp_view_009, global_temp_view_010, global_temp_view_011, external_location, out_repo_path, output_file_name_001, sep, header, '1'
# MAGIC
# MAGIC
# MAGIC def fix_created_file(file_name):
# MAGIC     try:
# MAGIC         full_file_name_aux = full_file_name + '_TEMP'
# MAGIC         dbutils.fs.rm(full_file_name_aux, recurse=True)
# MAGIC         file_name_tmp = dbutils.fs.ls(full_file_name)
# MAGIC         file_name_new = list(filter(lambda x: x[0].endswith('csv'), file_name_tmp))
# MAGIC         dbutils.fs.cp(file_name_new[0][0], full_file_name_aux)
# MAGIC         dbutils.fs.rm(full_file_name, recurse=True)
# MAGIC         dbutils.fs.cp(full_file_name_aux, full_file_name)
# MAGIC         dbutils.fs.rm(full_file_name_aux, recurse=True)
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: " + str(inspect.stack()[0][3]))
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC         return '0'
# MAGIC     return '1'
# MAGIC
# MAGIC if __name__ == "__main__" :
# MAGIC     logging.basicConfig()
# MAGIC     logger = logging.getLogger(__name__)
# MAGIC     logger.setLevel(logging.DEBUG)
# MAGIC     notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
# MAGIC     message = 'NB Error: ' + notebook_name
# MAGIC     source = 'ETL'
# MAGIC     root_repo = '/Workspace/Shared/MITAFO'
# MAGIC     
# MAGIC     try:
# MAGIC         sys.path.append(root_repo + '/' + 'CGRLS_0010/Notebooks')
# MAGIC         from NB_GRLS_DML_FUNCTIONS import *
# MAGIC         from NB_GRLS_SIMPLE_FUNCTIONS import *
# MAGIC     except Exception as e:
# MAGIC         logger.error("Error at the beggining of the process")
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC
# MAGIC     config_file = root_repo + '/' + 'CGRLS_0010/Conf/CF_GRLS_PROCESS.py.properties'
# MAGIC     config_conn_file = root_repo + '/' + 'CGRLS_0010/Conf/CF_GRLS_CONN.py.properties'
# MAGIC     config_process_file = root_repo + '/' + 'ANCIN_0030/Jobs/01_IDENTIFICACION_DE_CLIENTE/01_DISPERSIONES/Conf/CF_PART_PROC.py.properties'
# MAGIC
# MAGIC     sr_proceso, sr_subproceso, sr_subetapa, sr_origen_arc, sr_dt_org_arc, sr_folio, sr_id_archivo, sr_tipo_layout, sr_instancia_proceso, sr_usuario, sr_etapa, sr_id_snapshot, sr_paso, failed_task = input_values()
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         #notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("An error ocurred, check out log messages")
# MAGIC     
# MAGIC     input_parameters = dbutils.widgets.getAll().items()
# MAGIC     
# MAGIC     process_name = 'root'
# MAGIC     webhook_url, channel, failed_task = conf_init_values(config_file, process_name,'IDC')
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     process_name = 'root'
# MAGIC     sql_conf_file, temp_view_schema, global_temp_view_009, global_temp_view_010, global_temp_view_011, external_location, out_repo_path, output_file_name_001, sep, header, failed_task = conf_process_values(config_process_file, process_name)
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     conn_name_ora = 'jdbc_oracle'
# MAGIC     conn_options, conn_aditional_options, conn_user, conn_key, conn_url, failed_task = conf_conn_values(config_conn_file, conn_name_ora)
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC     
# MAGIC     sql_conf_file = root_repo + '/ANCIN_0030/Jobs/01_IDENTIFICACION_DE_CLIENTE/01_DISPERSIONES/JSON/' + sql_conf_file
# MAGIC

# COMMAND ----------

with open(sql_conf_file) as f:
    file_config_sql = json.load(f)

conf_values = [ (fields['step_id'], '\n'.join(fields['value'])) for line, value in file_config_sql.items() if line == 'steps' for fields in value ]

# COMMAND ----------

query_statement = '028'

global_temp_view_009 = temp_view_schema + '.' + global_temp_view_009 + '_' + sr_id_archivo
global_temp_view_010 = temp_view_schema + '.' + global_temp_view_010 + '_' + sr_id_archivo
global_temp_view_011 = temp_view_schema + '.' + global_temp_view_011 + '_' + sr_id_archivo

params = [global_temp_view_009, global_temp_view_010, global_temp_view_011]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

try:
    df = spark.sql(statement)
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
    df.createOrReplaceTempView(temp_view)
except Exception as e:
    logger.error("Function: %s", inspect.stack()[0][3])
    logger.error("An error was raised: " + str(e))
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    #raise Exception("An error raised")

# COMMAND ----------

query_statement = '029'

global_temp_view_aux = temp_view_schema + '.' + 'TEMP_SIEFORE01' + '_' + sr_id_archivo

params = [temp_view, global_temp_view_aux]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

try:
    df = spark.sql(statement)
except Exception as e:
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")


# COMMAND ----------

temp_view = 'TEMP_ARCHIVO_CLIENTES_005' + '_' + sr_id_archivo

#spark.catalog.dropGlobalTempView(temp_view)

try:
    df.createOrReplaceTempView(temp_view)
except Exception as e:
    logger.error("Function: %s", inspect.stack()[0][3])
    logger.error("An error was raised: " + str(e))
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)

# COMMAND ----------

query_statement = '030'

params = [temp_view]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

try:
    df = spark.sql(statement)
except Exception as e:
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")


# COMMAND ----------

temp_view = 'TEMP_ARCHIVO_CLIENTES_006' + '_' + sr_id_archivo

#spark.catalog.dropGlobalTempView(temp_view)

try:
    df.createOrReplaceTempView(temp_view)
except Exception as e:
    logger.error("Function: %s", inspect.stack()[0][3])
    logger.error("An error was raised: " + str(e))
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)

# COMMAND ----------

#Mismo que el 30
query_statement = '031'

params = [temp_view]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

try:
    df = spark.sql(statement)
except Exception as e:
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

# COMMAND ----------

#Crea la vista para realizar el pivote
temp_view = 'TEMP_ARCHIVO_CLIENTES_007' + '_' + sr_id_archivo

try:
    df.createOrReplaceTempView(temp_view)
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
except Exception as e:
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

# COMMAND ----------

temp_view = 'TEMP_ARCHIVO_CLIENTES_008' + '_' + sr_id_archivo
try:
    df.createOrReplaceTempView(temp_view)
except Exception as e:
    logger.error("Function: %s", inspect.stack()[0][3])
    logger.error("An error was raised: " + str(e))
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)

# COMMAND ----------

query_statement = '033'

params = ['TEMP_ARCHIVO_CLIENTES_006' + '_' + sr_id_archivo, 'TEMP_ARCHIVO_CLIENTES_008' + '_' + sr_id_archivo]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

try:
    df = spark.sql(statement)
except Exception as e:
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

# COMMAND ----------

full_file_name =  external_location + out_repo_path + '/' + sr_folio + '_' + output_file_name_001

file_name = full_file_name + '_TEMP'

try:
    df.write.format("csv").mode('overwrite').option("header", header).save(full_file_name + '_TEMP')
    dataframe = spark.read.option('header', header).csv(full_file_name + '_TEMP')
    dataframe.coalesce(1).write.format('csv').mode('overwrite').option('header', False).save(full_file_name)
except Exception as e:
    logger.error("An error was raised: " + str(e))
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

# COMMAND ----------

failed_task = fix_created_file(full_file_name)

if failed_task == '0':
        logger.error("Please review log messages")
        notification_raised(webhook_url, -1, message, source, input_parameters)
        raise Exception("Process ends")

# COMMAND ----------

notification_raised(webhook_url, 0, "DONE", source, input_parameters)
