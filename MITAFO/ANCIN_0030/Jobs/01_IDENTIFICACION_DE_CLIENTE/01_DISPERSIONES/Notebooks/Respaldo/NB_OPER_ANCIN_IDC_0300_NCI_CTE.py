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
# MAGIC         var_tramite = config.get(process_name, 'var_tramite')
# MAGIC         debug = config.get(process_name, 'debug')
# MAGIC         debug = debug.lower() == 'true'
# MAGIC  
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC         return '0','0','0','0'
# MAGIC     return sql_conf_file, var_tramite, debug, '1'
# MAGIC
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
# MAGIC     sql_conf_file, var_tramite, debug, failed_task = conf_process_values(config_process_file, process_name)
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
# MAGIC     sql_conf_file = root_repo + '/' + '/ANCIN_0030/Jobs/01_IDENTIFICACION_DE_CLIENTE/01_DISPERSIONES/JSON/' + sql_conf_file
# MAGIC

# COMMAND ----------

with open(sql_conf_file) as f:
    file_config_sql = json.load(f)

conf_values = [ (fields['step_id'], '\n'.join(fields['value'])) for line, value in file_config_sql.items() if line == 'steps' for fields in value ]

# COMMAND ----------

if var_tramite == 'DOIMSS':
    p_ind = '3'
elif var_tramite == 'DOISSSTE':
    p_ind = '2' 
else: 'error'

query_statement = '015'

params = [sr_folio,p_ind]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

df, failed_task = query_table(conn_name_ora, spark, statement,  conn_options, conn_user, conn_key)

if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")

print(debug)
print(type(debug)) 
if debug:
    print(statement)
    display(df)


# COMMAND ----------

# DBTITLE 1,Genera vista intermedia TEMP_IND_CTA_VIG
df1 = df.filter("FFN_ID_CONFIG_INDI = 2")

temp_view = 'TEMP_IND_CTA_VIG' + '_' + sr_id_archivo
print(temp_view)

spark.catalog.dropGlobalTempView(temp_view)
df1.createOrReplaceGlobalTempView(temp_view)

print(debug)    
if debug:
    display(df1)

# COMMAND ----------

# DBTITLE 1,Genera vista TEMP_FechaApCtaInd_05
df2 = df.select("FTN_NUM_CTA_INVDUAL","FCC_VALOR_IND").filter("FFN_ID_CONFIG_INDI = 5")

temp_view = 'TEMP_FechaApCtaInd_05' + '_' + sr_id_archivo
print(temp_view)
spark.catalog.dropGlobalTempView(temp_view)

df2.createOrReplaceGlobalTempView(temp_view)

print(debug)    
if debug:
    display(df2)

# COMMAND ----------

# DBTITLE 1,Genera vista TEMP_Pensionado_08
df3 = df.select("FTN_NUM_CTA_INVDUAL","FCC_VALOR_IND").filter("FFN_ID_CONFIG_INDI = 11")

temp_view = 'TEMP_Pensionado_08' + '_' + sr_id_archivo
print(temp_view)
spark.catalog.dropGlobalTempView(temp_view)

df3.createOrReplaceGlobalTempView(temp_view)
#spark.sql("select * from global_temp.TEMP_Pensionado_08_29288").show(10)

print(debug)    
if debug:
    display(df3)

# COMMAND ----------

query_statement = '016'

params = [sr_id_archivo]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

df_tramite = spark.sql(statement)

if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")


# COMMAND ----------

# DBTITLE 1,Genera Vista intermedia Temp_tramite
temp_view = 'Temp_tramite' + '_' + sr_id_archivo

# Para que???
#df_tramite.createOrReplaceTempView(temp_view)

print(debug)    
if debug:
    print(statement)
    display(df_tramite)

# COMMAND ----------

query_statement = '017'

params = [sr_id_archivo]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

df = spark.sql(statement)

if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")

print(debug)    
if debug:
    print(statement)
    display(df)

# COMMAND ----------

# DBTITLE 1,Genera TEMP_Vigentes_02
df1 = df.filter("FTN_NUM_CTA_INVDUAL IS NOT NULL AND FCC_VALOR_IND = 1")

temp_view = 'TEMP_Vigentes_02' + '_' + sr_id_archivo
print(temp_view)
spark.catalog.dropGlobalTempView(temp_view)

df1.createOrReplaceGlobalTempView(temp_view)

print(debug)    
if debug:
    display(df1)

# COMMAND ----------

# DBTITLE 1,Genera TEMP_NoVigentes_03
df2 = df.filter("FTN_NUM_CTA_INVDUAL IS NOT NULL AND FCC_VALOR_IND = 0")

temp_view = 'TEMP_NoVigentes_03' + '_' + sr_id_archivo
print(temp_view)
spark.catalog.dropGlobalTempView(temp_view)

df2.createOrReplaceGlobalTempView(temp_view)

print(debug)    
if debug:
    display(df2)

# COMMAND ----------

# DBTITLE 1,Genera TEMP_NoEncontrados_04
#375	INDICADOR NO ENCONTRADO
#Crea df3, filtra y agrega columnas 
df3 = df.filter("((FCC_VALOR_IND <> 1 and FCC_VALOR_IND <> 0) OR FCC_VALOR_IND IS NULL)")
df3 =  df3.select("FTN_NSS_CURP" ,"FTN_NUM_CTA_INVDUAL" ,"FTC_NOMBRE_BUC" ,"FTC_AP_PATERNO_BUC" ,"FTC_AP_MATERNO_BUC" ,"FTC_RFC_BUC" ,"FTC_FOLIO" ,"FTN_ID_ARCHIVO" ,"FTN_NSS" ,"FTC_CURP" ,"FTC_RFC" ,"FTC_NOMBRE_CTE" ,"FTC_CLAVE_ENT_RECEP", "FCN_ID_TIPO_SUBCTA", lit(0).alias("FTC_IDENTIFICADOS"), lit(375).alias("FTN_ID_DIAGNOSTICO"), lit(0).alias("FTN_VIGENCIA"))

if debug:
    display(df3)

#93	CLIENTE NO ENCONTRADO
#Agrega columnas FTC_IDENTIFICADOS, FTN_ID_DIAGNOSTICO, FTN_VIGENCIA
df_tramite = df_tramite.filter("FTC_BANDERA IS NULL")
df_tramite = df_tramite.select ("FTN_NSS_CURP" ,"FTN_NUM_CTA_INVDUAL" ,"FTC_NOMBRE_BUC" ,"FTC_AP_PATERNO_BUC" ,"FTC_AP_MATERNO_BUC" ,"FTC_RFC_BUC" ,"FTC_FOLIO" ,"FTN_ID_ARCHIVO" ,"FTN_NSS" ,"FTC_CURP" ,"FTC_RFC" ,"FTC_NOMBRE_CTE" ,"FTC_CLAVE_ENT_RECEP","FCN_ID_TIPO_SUBCTA", lit(0).alias("FTC_IDENTIFICADOS"), lit(93).alias("FTN_ID_DIAGNOSTICO"), lit(0).alias("FTN_VIGENCIA"))


#Union df_tramite y df3
df_NoEncontrados = df_tramite.union(df3)

temp_view = 'TEMP_NoEncontrados_04' + '_' + sr_id_archivo
spark.catalog.dropGlobalTempView(temp_view)

df_NoEncontrados.createOrReplaceGlobalTempView(temp_view)

if debug:
    display(df_NoEncontrados)
