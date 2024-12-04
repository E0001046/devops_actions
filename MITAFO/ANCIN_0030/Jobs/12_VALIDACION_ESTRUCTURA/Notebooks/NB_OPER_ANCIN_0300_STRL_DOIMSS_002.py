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
# MAGIC from pyspark.sql.functions import length, lit, current_timestamp, concat, col, when, lead
# MAGIC from pyspark.sql.window import Window
# MAGIC from pyspark import StorageLevel
# MAGIC
# MAGIC
# MAGIC def input_values():
# MAGIC     try:
# MAGIC         dbutils.widgets.text('sr_proceso', '')
# MAGIC         dbutils.widgets.text('sr_subproceso', '')
# MAGIC         dbutils.widgets.text('sr_subetapa', '')
# MAGIC         dbutils.widgets.text('sr_id_archivo', '')
# MAGIC         dbutils.widgets.text('sr_path_arch', '')
# MAGIC
# MAGIC         sr_proceso = dbutils.widgets.get('sr_proceso')
# MAGIC         sr_subproceso = dbutils.widgets.get('sr_subproceso')
# MAGIC         sr_subetapa = dbutils.widgets.get('sr_subetapa')
# MAGIC         sr_id_archivo = dbutils.widgets.get('sr_id_archivo')
# MAGIC         sr_path_arch = dbutils.widgets.get('sr_path_arch')
# MAGIC
# MAGIC         if any(len(str(value).strip()) == 0 for value in [sr_proceso, sr_subproceso, sr_subetapa, sr_id_archivo, sr_path_arch]):
# MAGIC             logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC             logger.error("Some of the input values are empty or null")
# MAGIC             return '0','0','0','0','0','0'
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC         return '0','0','0','0','0','0'
# MAGIC     return sr_proceso, sr_subproceso, sr_subetapa, sr_id_archivo, sr_path_arch, '1'
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
# MAGIC         storage_location = config.get(process_name, 'external_location')
# MAGIC         sql_conf_file = config.get(process_name, 'sql_conf_file')
# MAGIC         conn_schema_default = config.get(process_name, 'conn_schema_default')
# MAGIC         table_001 = config.get(process_name, 'table_001')
# MAGIC         val_max_length = config.get(process_name, 'val_records_length')
# MAGIC         val_control_order = config.get(process_name, 'val_control_order')
# MAGIC         #
# MAGIC         err_repo_path = config.get(process_name, 'err_repo_path')
# MAGIC         output_file_name_001 = config.get(process_name, 'output_file_name_001')
# MAGIC         sep = config.get(process_name, 'sep')
# MAGIC         header = config.get(process_name, 'header')
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC         return '0','0','0','0','0','0','0','0','0','0'
# MAGIC     return storage_location, sql_conf_file, conn_schema_default, table_001, val_max_length, val_control_order, err_repo_path, output_file_name_001, sep, header, '1'
# MAGIC
# MAGIC
# MAGIC def load_file(storage_location, sr_path_arch):
# MAGIC     """ Load a file from Azure Blob Storage.
# MAGIC     Args:
# MAGIC         blob_storage (str): Blob storage name.
# MAGIC         storage_account (str): Storage account name.
# MAGIC         url_domain (str): URL domain.
# MAGIC         sr_path_arch (str): Path to the file in the storage.
# MAGIC     Returns:
# MAGIC         tuple: A tuple containing the DataFrame and a status flag.
# MAGIC     """
# MAGIC     try:
# MAGIC         #dbutils.fs.ls(storage_location + sr_path_arch)
# MAGIC         schema = StructType([StructField('FTC_LINEA', StringType(), True)])
# MAGIC         pdf = spark.read.csv(storage_location + sr_path_arch, schema = schema).toPandas()
# MAGIC         pdf = pdf.assign(FTN_NO_LINEA=range(1, len(pdf) + 1))
# MAGIC         df = spark.createDataFrame(pdf)
# MAGIC         del pdf
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC         return '0', '0'
# MAGIC     return df, '1'
# MAGIC
# MAGIC
# MAGIC def aditional_values(df, sr_id_archivo, sr_subetapa):
# MAGIC     """ Add additional values to the DataFrame.
# MAGIC     Args:
# MAGIC         df (DataFrame): Input DataFrame.
# MAGIC         sr_id_archivo (str): File ID.
# MAGIC     Returns:
# MAGIC         tuple: A tuple containing the modified DataFrame and a status flag.
# MAGIC     """
# MAGIC     try:
# MAGIC         df = df.withColumn('SR_ID_ARCHIVO', lit(sr_id_archivo)) \
# MAGIC                 .withColumn('SR_SUBETAPA', lit(sr_subetapa)) \
# MAGIC                 .withColumn('FTC_FOLIO', lit('')) \
# MAGIC                 .withColumn('FECHA_CARGA', current_timestamp()) \
# MAGIC                 .withColumn('FTC_CONTROL', df['FTC_LINEA'].substr(1, 2))
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC         return '0', '0'
# MAGIC     return df, '1'
# MAGIC
# MAGIC
# MAGIC def validation_task(dfValTsk):
# MAGIC     try:
# MAGIC         dfValTsk = dfValTsk.withColumn('FTC_LONG_REG', length(dfValTsk['FTC_LINEA'])) \
# MAGIC                     .withColumn('VAL_LENGTH_RECORD', when(length(dfValTsk['FTC_LINEA']) == val_max_length, 1).otherwise(0))
# MAGIC         
# MAGIC         windowSpec  = Window.partitionBy("SR_ID_ARCHIVO").orderBy("FTN_NO_LINEA")
# MAGIC         dfValTsk = dfValTsk.withColumn("SIG_CONTROL", lead("FTC_CONTROL", 1, '').over(windowSpec))
# MAGIC         dfValTsk = dfValTsk.withColumn("VAL_ESTATUS", \
# MAGIC                         when((dfValTsk['FTC_CONTROL'] == '01') & (dfValTsk['SIG_CONTROL'] == '02'), 1) \
# MAGIC                             .when((dfValTsk['FTC_CONTROL'] == '02') & ((dfValTsk['SIG_CONTROL'] == '02') | (dfValTsk['SIG_CONTROL'] == '08')), 1) \
# MAGIC                             .when((dfValTsk['FTC_CONTROL'] == '08') & ((dfValTsk['SIG_CONTROL'] == '08') | (dfValTsk['SIG_CONTROL'] == '09')), 1) \
# MAGIC                             .when((dfValTsk['FTC_CONTROL'] == '09') & (dfValTsk['SIG_CONTROL'] == ''), 1) \
# MAGIC                             .otherwise(0)
# MAGIC                         )
# MAGIC
# MAGIC         dfValTsk = dfValTsk.withColumn("SIG_LINEA", \
# MAGIC                         when(dfValTsk['VAL_ESTATUS'] == 0, lead("FTC_LINEA", 1, '').over(windowSpec))
# MAGIC                         .otherwise('')
# MAGIC                         )
# MAGIC         
# MAGIC         dfValTsk.persist(StorageLevel.DISK_ONLY)
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC         return '0', '0'
# MAGIC     return dfValTsk, '1'
# MAGIC
# MAGIC
# MAGIC
# MAGIC def fix_created_file(file_name):
# MAGIC     try:
# MAGIC         file_name_tmp = dbutils.fs.ls(file_name + '_TEMP')
# MAGIC         file_name_new = list(filter(lambda x : x[0].endswith('csv'), file_name_tmp))
# MAGIC         dbutils.fs.mv(file_name_new[0][0], file_name)
# MAGIC         dbutils.fs.rm(file_name + '_TEMP', recurse=True)
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: " + str(inspect.stack()[0][3]))
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC         return '0'
# MAGIC     return '1'
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
# MAGIC
# MAGIC     config_file = root_repo + '/' + 'CGRLS_0010/Conf/CF_GRLS_PROCESS.py.properties'
# MAGIC     config_conn_file = root_repo + '/' + 'CGRLS_0010/Conf/CF_GRLS_CONN.py.properties'
# MAGIC     config_process_file = root_repo + '/' + 'ANCIN_0030/Jobs/12_VALIDACION_ESTRUCTURA/Conf/CF_PART_PROC.py.properties'
# MAGIC
# MAGIC     sr_proceso, sr_subproceso, sr_subetapa, sr_id_archivo, sr_path_arch, failed_task = input_values()
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         #notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("An error ocurred, check out log messages")
# MAGIC     
# MAGIC     input_parameters = dbutils.widgets.getAll().items()
# MAGIC
# MAGIC     process_name = 'root'
# MAGIC     
# MAGIC     webhook_url, channel, failed_task = conf_init_values(config_file, process_name, 'VAL_ESTRUCT')
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC     
# MAGIC
# MAGIC     conn_name_ora = 'jdbc_oracle'
# MAGIC     conn_options, conn_aditional_options, conn_user, conn_key, conn_url, failed_task = conf_conn_values(config_conn_file, conn_name_ora)
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC     
# MAGIC     
# MAGIC     process_name = 'root'
# MAGIC     storage_location, sql_conf_file, conn_schema_default, table_001, val_max_length, val_control_order, err_repo_path, output_file_name_001, sep, header, failed_task = conf_process_values(config_process_file, process_name)
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC     
# MAGIC     sql_conf_file = root_repo + '/' + 'ANCIN_0030/Jobs/12_VALIDACION_ESTRUCTURA/JSON/' + sql_conf_file
# MAGIC
# MAGIC     #empty_df = sqlContext.createDataFrame(sc.emptyRDD(), schema = StructType([]))

# COMMAND ----------

with open(sql_conf_file) as f:
    file_config_sql = json.load(f)

conf_values = [ (fields['step_id'], '\n'.join(fields['value'])) for line, value in file_config_sql.items() if line == 'steps' for fields in value ]


# COMMAND ----------

query_statement = '001'

temp_view = "GLOBAL_TEMP" + "." + "TEMP_VALIDACION_ESTRUCTURA_ARCHIVO" + "_" + sr_id_archivo

params = [temp_view]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

df_output_001 = spark.sql(statement)

# COMMAND ----------

full_file_name =  storage_location + err_repo_path + '/' + sr_id_archivo + '_' + output_file_name_001

try:
    df_output_001.write.format("csv").mode('append').option("header", header).save(full_file_name + '_TEMP')
except Exception as e:
    logger.error("An error was raised: " + str(e))
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")
