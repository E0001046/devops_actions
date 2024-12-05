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
# MAGIC
# MAGIC from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
# MAGIC from pyspark.sql.functions import length, lit
# MAGIC
# MAGIC
# MAGIC def input_values():
# MAGIC     """ Retrieve input values from widgets.
# MAGIC     Returns:
# MAGIC         tuple: A tuple containing the input values and a status flag.
# MAGIC     """
# MAGIC     try:
# MAGIC         dbutils.widgets.text('sr_tipo_reporte', '')
# MAGIC         dbutils.widgets.text('sr_proceso', '')
# MAGIC         dbutils.widgets.text('sr_subproceso', '')
# MAGIC         dbutils.widgets.text('sr_origen_arc', '')
# MAGIC         dbutils.widgets.text('sr_dt_org_arc', '')
# MAGIC         dbutils.widgets.text('sr_subetapa', '')
# MAGIC         dbutils.widgets.text('sr_folio', '')
# MAGIC         dbutils.widgets.text('sr_id_archivo', '')
# MAGIC         dbutils.widgets.text('sr_fec_arc', '')
# MAGIC         dbutils.widgets.text('sr_fec_liq', '')
# MAGIC         dbutils.widgets.text('sr_usuario','')
# MAGIC         dbutils.widgets.text('sr_etapa','')
# MAGIC         dbutils.widgets.text('sr_id_snapshot','')
# MAGIC         dbutils.widgets.text('sr_instancia_proceso','')
# MAGIC
# MAGIC         sr_tipo_reporte = dbutils.widgets.get('sr_tipo_reporte')
# MAGIC         sr_proceso = dbutils.widgets.get('sr_proceso')
# MAGIC         sr_subproceso = dbutils.widgets.get('sr_subproceso')
# MAGIC         sr_origen_arc = dbutils.widgets.get('sr_origen_arc')
# MAGIC         sr_dt_org_arc = dbutils.widgets.get('sr_dt_org_arc')
# MAGIC         sr_subetapa = dbutils.widgets.get('sr_subetapa')
# MAGIC         sr_folio = dbutils.widgets.get('sr_folio')
# MAGIC         sr_id_archivo = dbutils.widgets.get('sr_id_archivo')
# MAGIC         sr_fec_arc = dbutils.widgets.get('sr_fec_arc')
# MAGIC         sr_fec_liq = dbutils.widgets.get('sr_fec_liq')
# MAGIC         sr_usuario = dbutils.widgets.get('sr_usuario')
# MAGIC         sr_etapa = dbutils.widgets.get('sr_etapa')
# MAGIC         sr_id_snapshot = dbutils.widgets.get('sr_id_snapshot')
# MAGIC         sr_instancia_proceso = dbutils.widgets.get('sr_instancia_proceso')
# MAGIC
# MAGIC         if any(len(str(value).strip()) == 0 for value in [sr_tipo_reporte, sr_proceso, sr_subproceso, sr_origen_arc, sr_dt_org_arc, sr_subetapa, sr_folio, sr_id_archivo, sr_fec_arc, sr_fec_liq,sr_usuario,sr_etapa,sr_id_snapshot,sr_instancia_proceso]):    
# MAGIC             logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC             logger.error("Some of the input values are empty or null")
# MAGIC             return '0','0','0','0','0','0','0','0','0','0','0','0','0','0'
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: %s", str(e))
# MAGIC         return '0','0','0','0','0','0','0','0','0','0','0','0','0','0'
# MAGIC     return sr_tipo_reporte, sr_proceso, sr_subproceso, sr_origen_arc, sr_dt_org_arc, sr_subetapa, sr_folio, sr_id_archivo, sr_fec_arc, sr_fec_liq,sr_usuario,sr_etapa,sr_id_snapshot,sr_instancia_proceso, '1'
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
# MAGIC         table_001 = config.get(process_name, 'table_001')
# MAGIC         table_002 = config.get(process_name, 'table_002')
# MAGIC         conn_schema_002 = config.get(process_name, 'conn_schema_002')
# MAGIC         table_003 = config.get(process_name, 'table_003')
# MAGIC         conn_schema_003 = config.get(process_name, 'conn_schema_003')
# MAGIC         table_004 = config.get(process_name, 'table_004')
# MAGIC         var_001 = config.get(process_name, 'var_001')
# MAGIC         var_002 = config.get(process_name, 'var_002')
# MAGIC         conn_schema_004 = config.get(process_name, 'conn_schema_004')
# MAGIC         table_005 = config.get(process_name, 'table_005')
# MAGIC         table_006 = config.get(process_name, 'table_006')
# MAGIC         table_007 = config.get(process_name, 'table_007')
# MAGIC         table_008 = config.get(process_name, 'table_008')
# MAGIC         table_009 = config.get(process_name, 'table_009')
# MAGIC         table_010 = config.get(process_name, 'table_010')
# MAGIC         table_011 = config.get(process_name, 'table_011')
# MAGIC         table_012 = config.get(process_name, 'table_012')
# MAGIC         table_013 = config.get(process_name, 'table_013')
# MAGIC         table_014 = config.get(process_name, 'table_014')
# MAGIC         table_015 = config.get(process_name, 'table_015')
# MAGIC         table_016 = config.get(process_name, 'table_016')
# MAGIC         table_017 = config.get(process_name, 'table_017')
# MAGIC         table_018 = config.get(process_name, 'table_018')
# MAGIC         var_003 = config.get(process_name, 'var_003')
# MAGIC         var_004 = config.get(process_name, 'var_004')
# MAGIC         table_019 = config.get(process_name, 'table_019')
# MAGIC         table_020 = config.get(process_name, 'table_020')
# MAGIC         var_005 = config.get(process_name, 'var_005')
# MAGIC         table_021 = config.get(process_name, 'table_021')
# MAGIC         table_022 = config.get(process_name, 'table_022')
# MAGIC
# MAGIC
# MAGIC
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC         return '0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0'
# MAGIC     return sql_conf_file, conn_schema_001, table_001, table_002, conn_schema_002, table_003, conn_schema_003, var_001, var_002, conn_schema_004, table_005, table_006, table_007, table_008, table_009, table_010, table_011, table_012, table_013, table_014,table_015,table_016,table_017,table_018,var_003, var_004,table_019,table_020,var_005,table_021,table_022, '1'
# MAGIC
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
# MAGIC     config_process_file = root_repo + '/' + 'AGRLS_0010/Jobs/03_CONCILIACION/02_CIFRAS_CONTROL/Conf/CF_PART_PROC.py.properties'
# MAGIC
# MAGIC     
# MAGIC     sr_tipo_reporte, sr_proceso, sr_subproceso, sr_origen_arc, sr_dt_org_arc, sr_subetapa, sr_folio, sr_id_archivo, sr_fec_arc, sr_fec_liq, sr_usuario,sr_etapa,sr_id_snapshot,sr_instancia_proceso, failed_task = input_values()
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("An error ocurred, check out log messages")
# MAGIC     
# MAGIC     input_parameters = dbutils.widgets.getAll().items()
# MAGIC     
# MAGIC     process_name = 'root'
# MAGIC     webhook_url, channel, failed_task = conf_init_values(config_file, process_name,'GCC')
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     process_name = 'root'
# MAGIC     sql_conf_file, conn_schema_001, table_001, table_002, conn_schema_002, table_003,conn_schema_003,var_001,var_002,conn_schema_004,table_005,table_006,table_007,table_008,table_009,table_010,table_011,table_012,table_013,table_014,table_015,table_016,table_017,table_018,var_003, var_004,table_019,table_020,var_005,table_021,table_022, failed_task = conf_process_values(config_process_file, process_name)
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     conn_name_ora = 'jdbc_oracle'
# MAGIC     conn_options, conn_aditional_options, conn_user, conn_key, conn_url, scope, failed_task = conf_conn_values(config_conn_file, conn_name_ora)
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     sql_conf_file = root_repo + '/' + 'AGRLS_0010/Jobs/03_CONCILIACION/02_CIFRAS_CONTROL/JSON/' + sql_conf_file
# MAGIC

# COMMAND ----------

with open(sql_conf_file) as f:
    file_config_sql = json.load(f)

conf_values = [ (fields['step_id'], '\n'.join(fields['value'])) for line, value in file_config_sql.items() if line == 'steps' for fields in value ]

# COMMAND ----------

# DBTITLE 1,2DA PARTE DE LA EXTRACCIÓN - CONCILIACIONES
#conn_schema_001 : CIERREN_ETL
#table_001 : TTSISGRAL_ETL_DISPERSION
#table_002 : TLSISGRAL_ETL_VAL_MATRIZ_CONV
#conn_schema_002 : CIERREN
#table_003 : TFAFOGRAL_CONFIG_CONCEP_MOV
#conn_schema_003 : PROCESOS
#table_004 : TTAFOAE_AEIM

var_001 = var_001
var_002 = var_002
table_name_002 = conn_schema_001 + '.' + table_002
table_name_003 = conn_schema_002 + '.' + table_003
table_name_005 = conn_schema_004 + '.' + table_005
table_name_006 = conn_schema_002 + '.' + table_006
table_name_007 = conn_schema_004 + '.' + table_007
table_name_008 = conn_schema_004 + '.' + table_008
table_name_009 = conn_schema_004 + '.' + table_009
table_name_010 = conn_schema_002 + '.' + table_010
table_name_011 = conn_schema_002 + '.' + table_011
table_name_012 = conn_schema_002 + '.' + table_012
table_name_013 = conn_schema_002 + '.' + table_013
table_name_014 = conn_schema_002 + '.' + table_014
table_name_015 = conn_schema_001 + '.' + table_015
table_name_016 = conn_schema_004 + '.' + table_016
table_name_017 = conn_schema_004 + '.' + table_017
table_name_018 = conn_schema_002 + '.' + table_018
var_003 = var_003
var_004 = var_004
table_name_019 = conn_schema_004 + '.' + table_019
table_name_020 = conn_schema_003 + '.' + table_020
var_005 = var_005
table_name_021 = conn_schema_004 + '.' + table_021
table_name_022 = conn_schema_002 + '.' + table_022


query_statement = '002'

params = [var_001,var_002,table_name_005,table_name_006,table_name_007,table_name_008,sr_folio,table_name_009,table_name_010,table_name_011,table_name_012,table_name_013,table_name_014,table_name_015,table_name_016,table_name_017,table_name_018,var_003,var_004,table_name_019,table_name_020,var_005,table_name_021,table_name_002,table_name_022,table_name_003]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

# COMMAND ----------

# DBTITLE 1,CONSTRUCCIÓN DE CONSULTA
df, failed_task = query_table(conn_name_ora, spark, statement,  conn_options, conn_user, conn_key)

# COMMAND ----------

# DBTITLE 1,ASIGNACIÓN DE VISTA
temp_view = "TEMP_CONCILIACIONES_CIFRAS" + '_' + sr_id_archivo

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# DBTITLE 1,CREACIÓN DE VISTA
from pyspark.sql.types import StructType
if not df.head(1): # es mejor usar head y no count para validar si el df tiene datos o no
    schema = df.schema
    df_vacio = spark.createDataFrame([], schema)
    # Registrar la vista global vacía
    df_vacio.createOrReplaceGlobalTempView(temp_view)

# COMMAND ----------

# DBTITLE 1,VALIDACIÓN
#Solo como validación para poder ver los datos, no es necesario.
df_vacio.printSchema()
