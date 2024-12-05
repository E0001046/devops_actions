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
# MAGIC from datetime import datetime, timezone
# MAGIC import pytz
# MAGIC
# MAGIC from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
# MAGIC from pyspark.sql.functions import length, lit, expr, concat, when
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
# MAGIC         dbutils.widgets.text('sr_folio', '')
# MAGIC         dbutils.widgets.text('sr_id_archivo', '')
# MAGIC         dbutils.widgets.text('sr_instancia_proceso', '')
# MAGIC         dbutils.widgets.text('sr_usuario', '')
# MAGIC         dbutils.widgets.text('sr_etapa', '')
# MAGIC         dbutils.widgets.text('sr_id_snapshot', '')
# MAGIC
# MAGIC         sr_proceso = dbutils.widgets.get('sr_proceso')
# MAGIC         sr_subproceso = dbutils.widgets.get('sr_subproceso')
# MAGIC         sr_subetapa = dbutils.widgets.get('sr_subetapa')
# MAGIC         sr_folio = dbutils.widgets.get('sr_folio')
# MAGIC         sr_id_archivo = dbutils.widgets.get('sr_id_archivo')
# MAGIC         sr_instancia_proceso = dbutils.widgets.get('sr_instancia_proceso')
# MAGIC         sr_usuario = dbutils.widgets.get('sr_usuario')
# MAGIC         sr_etapa = dbutils.widgets.get('sr_etapa')
# MAGIC         sr_id_snapshot = dbutils.widgets.get('sr_id_snapshot')
# MAGIC
# MAGIC         if any(len(str(value).strip()) == 0 for value in [sr_proceso, sr_subproceso, sr_subetapa, sr_folio, sr_id_archivo, sr_instancia_proceso, sr_usuario, sr_etapa, sr_id_snapshot]):
# MAGIC             logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC             logger.error("Some of the input values are empty or null")
# MAGIC             return '0','0','0','0','0','0','0','0','0','0'
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: %s", str(e))
# MAGIC         return '0','0','0','0','0','0','0','0','0','0'
# MAGIC     return sr_proceso, sr_subproceso, sr_subetapa, sr_folio, sr_id_archivo, sr_instancia_proceso, sr_usuario, sr_etapa, sr_id_snapshot, '1'
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
# MAGIC         conn_schema_001 = config.get(process_name, 'conn_schema_001')
# MAGIC         table_005 = config.get(process_name, 'table_005')
# MAGIC         global_temp_view_schema_001 = config.get(process_name, 'global_temp_view_schema_001')
# MAGIC         global_temp_view_004 = config.get(process_name, 'global_temp_view_004')
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC         return '0','0','0','0','0','0','0'
# MAGIC     return sql_conf_file, conn_schema_001, table_005, global_temp_view_schema_001, global_temp_view_004, '1'
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
# MAGIC     config_process_file = root_repo + '/' + 'AGRLS_0010/Jobs/02_INTERPR_CONTENIDO/Conf/CF_PART_PROC.py.properties'
# MAGIC
# MAGIC     sr_proceso, sr_subproceso, sr_subetapa, sr_folio, sr_id_archivo, sr_instancia_proceso, sr_usuario, sr_etapa, sr_id_snapshot, failed_task = input_values()
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         #notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("An error ocurred, check out log messages")
# MAGIC     
# MAGIC     input_parameters = dbutils.widgets.getAll().items()
# MAGIC     
# MAGIC     process_name = 'root'
# MAGIC     webhook_url, channel, failed_task = conf_init_values(config_file, process_name, 'INTER_CONT')
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     process_name = 'root'
# MAGIC     sql_conf_file, conn_schema_001, table_005, global_temp_view_schema_001, global_temp_view_004, failed_task = conf_process_values(config_process_file, process_name)
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     conn_name_ora = 'jdbc_oracle'
# MAGIC     conn_options, conn_aditional_options, conn_user, conn_key, conn_url,scope,failed_task = conf_conn_values(config_conn_file, conn_name_ora)
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC     
# MAGIC     sql_conf_file = root_repo + '/' + 'AGRLS_0010/Jobs/02_INTERPR_CONTENIDO/JSON/' + sql_conf_file
# MAGIC

# COMMAND ----------

with open(sql_conf_file) as f:
    file_config_sql = json.load(f)

conf_values = [ (fields['step_id'], '\n'.join(fields['value'])) for line, value in file_config_sql.items() if line == 'steps' for fields in value ]

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

now = datetime.datetime.now(pytz.timezone('US/Central'))
fnd_feh_cre = now.strftime('%d/%m/%Y %H:%M:%S')


# COMMAND ----------

#conn_schema_001 : CIERREN_ETL
#table_001 : TTSISGRAL_ETL_LEE_ARCHIVO
#global_temp_view_schema_001 : global_temp
#global_temp_view_001 : TEMP_ETL_LEE_ARCHIVO
#global_temp_view_002 : TEMP_CAT_SUBCTA
#global_temp_view_003 : TEMP_JOIN_LEE_CAT

temp_view_004 = global_temp_view_schema_001 + '.' + global_temp_view_004 + '_' + sr_id_archivo

query_statement = '006'

params = [temp_view_004, sr_folio, fnd_feh_cre, 'DATABRICKS', sr_proceso, sr_subproceso, '02']

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

# COMMAND ----------

df = spark.sql(statement)

if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")

df.createOrReplaceTempView("TEMP_SUA")

df = spark.sql("SELECT FNN_ID_REFERENCIA ,FTC_FOLIO ,FTN_NSS ,FTC_RFC ,FTC_CURP ,FTC_NOMBRE ,FNC_PERIODO_PAGO_PATRON ,FND_FECHA_PAGO ,FND_FECHA_VALOR_IMSS_ACV_VIV ,FNN_ULTIMO_SALARIO_INT_PER ,FNC_FOLIO_PAGO_SUA ,FNC_REG_PATRONAL_IMSS ,FNC_RFC_PATRON ,FNN_DIAS_COTZDOS_BIMESTRE ,FNN_DIAS_INCAP_BIMESTRE ,FNN_DIAS_AUSENT_BIMESTRE ,FNN_ID_VIV_GARANTIA ,FNN_APLIC_INT_VIVIENDA ,FNC_MOTIVO_LIBCION_APORT ,FND_FECHA_PAGO_CUOTA_GUB ,FND_FECHA_VALOR_RCV ,FNN_CVE_ENT_RECEP_PAGO ,FNN_NSS_ISSSTE ,FNC_DEPEND_CTRO_PAGO ,FNN_CTRO_PAGO_SAR ,FNN_CVE_RAMO ,FNC_CVE_PAGADURIA ,FNN_SUELDO_BASICO_COTIZ_RCV ,FNC_ID_TRABAJADOR_BONO ,FND_FEH_CRE ,FNC_USU_CRE ,FTN_INTERESES_VIV92 ,FTN_INTERESES_VIV08 ,FCN_ID_PROCESO ,FCN_ID_SUBPROCESO ,FNC_TIPREG ,FND_FECTRA ,FNN_SECLOT ,FNN_ID_ARCHIVO ,FTN_CONS_REG_LOTE ,FCN_ID_TIPO_APO FROM TEMP_SUA")

print(statement)
display(df)

# COMMAND ----------

mode = 'APPEND'

table_name_001 = conn_schema_001 + '.' + table_005

failed_task = write_into_table(conn_name_ora, df, mode, table_name_001, conn_options, conn_aditional_options, conn_user, conn_key)

if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")
