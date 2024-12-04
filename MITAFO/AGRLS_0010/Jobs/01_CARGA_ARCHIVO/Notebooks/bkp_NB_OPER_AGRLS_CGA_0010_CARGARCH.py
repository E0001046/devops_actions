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
# MAGIC     try:
# MAGIC         dbutils.widgets.text('sr_proceso', '')
# MAGIC         dbutils.widgets.text('sr_subproceso', '')
# MAGIC         dbutils.widgets.text('sr_subetapa', '')
# MAGIC         dbutils.widgets.text('sr_id_archivo', '')
# MAGIC         dbutils.widgets.text('sr_path_arch', '')
# MAGIC         dbutils.widgets.text('sr_origen_arc', '')
# MAGIC         dbutils.widgets.text('root_repo', '')
# MAGIC
# MAGIC         sr_proceso = dbutils.widgets.get('sr_proceso')
# MAGIC         sr_subproceso = dbutils.widgets.get('sr_subproceso')
# MAGIC         sr_subetapa = dbutils.widgets.get('sr_subetapa')
# MAGIC         sr_id_archivo = dbutils.widgets.get('sr_id_archivo')
# MAGIC         sr_path_arch = dbutils.widgets.get('sr_path_arch')
# MAGIC         sr_origen_arc = dbutils.widgets.get('sr_origen_arc')
# MAGIC         root_repo = dbutils.widgets.get('root_repo')
# MAGIC
# MAGIC         if any(len(str(value).strip()) == 0 for value in [sr_proceso, sr_subproceso, sr_subetapa, sr_id_archivo, sr_path_arch, sr_origen_arc, root_repo]):
# MAGIC             logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC             logger.error("Some of the input values are empty or null")
# MAGIC             return '0','0','0','0','0','0','0','0'
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC         return '0','0','0','0','0','0','0','0'
# MAGIC     return sr_proceso, sr_subproceso, sr_subetapa, sr_id_archivo, sr_path_arch, sr_origen_arc, root_repo, '1'
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
# MAGIC         #process_conf_path = config.get(process_name, 'conf_file')
# MAGIC         val_max_length = config.get(process_name, 'val_records_length')
# MAGIC         val_control_order = config.get(process_name, 'val_control_order')
# MAGIC         table_001 = config.get(process_name, 'table_001')
# MAGIC         table_partition_col_001 = config.get(process_name, 'table_partition_col_001')
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC         return '0','0','0','0','0'
# MAGIC     return val_max_length, val_control_order, table_001, table_partition_col_001, '1'
# MAGIC
# MAGIC
# MAGIC def load_file(sr_path_arch):
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
# MAGIC         schema = StructType([StructField('FTC_LINEA', StringType(), True)])
# MAGIC         #full_connection = "wasbs" + "://" + blob_storage + "@" + storage_account + "." + url_domain + "/"
# MAGIC         #full_connection = f"wasbs://{blob_storage}@{storage_account}.{url_domain}/"
# MAGIC         full_connection = '/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/'
# MAGIC         df = spark.read.csv(full_connection + sr_path_arch, schema = schema)
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC         return '0', '0'
# MAGIC     return df, '1'
# MAGIC
# MAGIC
# MAGIC def aditional_values(df, sr_proceso, sr_subproceso, sr_subetapa, sr_id_archivo, sr_path_arch, sr_origen_arc):
# MAGIC     """ Add additional values to the DataFrame.
# MAGIC     Args:
# MAGIC         df (DataFrame): Input DataFrame.
# MAGIC         sr_proceso (str): Process ID.
# MAGIC         sr_subproceso (str): Subprocess ID.
# MAGIC         sr_subetapa (str): Substage ID.
# MAGIC         sr_id_archivo (str): File ID.
# MAGIC         sr_path_arch (str): File path.
# MAGIC         sr_origen_arc (str): File origin.
# MAGIC     Returns:
# MAGIC         tuple: A tuple containing the modified DataFrame and a status flag.
# MAGIC     """
# MAGIC     try:
# MAGIC         #df = df.withColumn('FCN_ID_PROCESO', lit(sr_proceso))
# MAGIC         #df = df.withColumn('FCN_ID_SUBPROCESO', lit(sr_subproceso))
# MAGIC         #df = df.withColumn('FCN_ID_SUBETAPA', lit(sr_subetapa))
# MAGIC         df = df.withColumn('FTN_ID_ARCHIVO', lit(sr_id_archivo))
# MAGIC         #df = df.withColumn('FCN_ID_PATH_ARCH', lit(sr_path_arch))
# MAGIC         #df = df.withColumn('FCN_ID_ORIGEN_ARCH', lit(sr_origen_arc))
# MAGIC         df = df.withColumn('FTC_FOLIO', lit(None).cast(StringType()))
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC         return '0', '0'
# MAGIC     return df, '1'
# MAGIC
# MAGIC
# MAGIC def validations_tasks(df, val_max_length, val_control_order):
# MAGIC     """
# MAGIC     Valida las tareas en el DataFrame.
# MAGIC     Args:
# MAGIC         df (DataFrame): El DataFrame a validar.
# MAGIC         val_max_length (int): Longitud máxima permitida.
# MAGIC         val_control_order (str): Orden de control.
# MAGIC     Returns:
# MAGIC         DataFrame: El DataFrame validado.
# MAGIC     """
# MAGIC     try:
# MAGIC         #Building data
# MAGIC         column_names = df.columns
# MAGIC         pdf = df.toPandas()
# MAGIC         pdf = pdf.assign(FTN_NO_LINEA=range(1, len(pdf) + 1))
# MAGIC         pdf["FTC_CONTROL"] = pdf.apply(lambda x : x[column_names[0]][0:2], axis = 1)
# MAGIC         
# MAGIC         control_order = pdf["FTC_CONTROL"].unique()
# MAGIC
# MAGIC         df = spark.createDataFrame(pdf)
# MAGIC
# MAGIC         df = df.withColumn('FTC_LONG_REG', length(df['FTC_LINEA']))
# MAGIC
# MAGIC         #diff_length = df.filter('FTC_LONG_REG != ' + val_max_length).count()
# MAGIC         diff_length = df.filter(f'FTC_LONG_REG != {val_max_length}').count()
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC         return '0', '0'
# MAGIC
# MAGIC     if diff_length != 0:
# MAGIC         logger.error("Found records having different length")
# MAGIC         logger.error("Default length: %s", str(val_max_length))
# MAGIC         logger.error("Total records: %s", str(diff_length))
# MAGIC         del pdf
# MAGIC         return '0', '0'
# MAGIC     
# MAGIC     if str(control_order).replace('[', '').replace(']','') != val_control_order.replace(',',' '):
# MAGIC         logger.error("Out of order")
# MAGIC         logger.error("Expected order: %s", val_control_order)
# MAGIC         logger.error("Got order: %s", str(control_order).replace('[', '').replace(']',''))
# MAGIC         del pdf
# MAGIC         return '0', '0'
# MAGIC     del pdf
# MAGIC     return df, '1'
# MAGIC
# MAGIC
# MAGIC if __name__ == "__main__" :
# MAGIC     logging.basicConfig()
# MAGIC     logger = logging.getLogger(__name__)
# MAGIC     logger.setLevel(logging.DEBUG)
# MAGIC     notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
# MAGIC     message = 'NB Error: ' + notebook_name
# MAGIC     source = 'ETL'
# MAGIC     
# MAGIC     sr_proceso, sr_subproceso, sr_subetapa, sr_id_archivo, sr_path_arch, sr_origen_arc, root_repo, failed_task = input_values()
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         raise Exception("An error ocurred, check out log messages")
# MAGIC     
# MAGIC     input_parameters = dbutils.widgets.getAll().items()
# MAGIC
# MAGIC     sys.path.append(root_repo + '/' + 'CGRLS_0010/Notebooks')
# MAGIC     from NB_GRLS_DML_FUNCTIONS import *
# MAGIC     from NB_GRLS_SIMPLE_FUNCTIONS import *
# MAGIC
# MAGIC     config_file = root_repo + '/' + 'CGRLS_0010/Conf/CF_GRLS_PROCESS.py.properties'
# MAGIC     #config_conn_file = root_repo + '/' + 'conf/config_processes.py.properties'
# MAGIC
# MAGIC     process_name = 'root'
# MAGIC     
# MAGIC     webhook_url, channel, failed_task = conf_init_values(config_file, process_name)
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     config_conn_file = root_repo + '/' + 'CGRLS_0010/Conf/CF_GRLS_CONN.py.properties'
# MAGIC
# MAGIC     conn_name_ora = 'jdbc_oracle'
# MAGIC     conn_options, conn_aditional_options, conn_user, conn_key, failed_task = conf_conn_values(config_conn_file, conn_name_ora)
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC     
# MAGIC     config_process_file = root_repo + '/' + 'AGRLS_0010/Jobs/01_CARGA_ARCHIVO/Conf/CF_PART_PROC.py.properties'
# MAGIC     
# MAGIC     process_name = 'root'
# MAGIC     
# MAGIC     val_max_length, val_control_order, table_001, table_partition_col_001, failed_task = conf_process_values(config_process_file, process_name)
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC     
# MAGIC     #empty_df = sqlContext.createDataFrame(sc.emptyRDD(), schema = StructType([]))

# COMMAND ----------

#%load_ext autoreload
#%autoreload 2
table_name = table_001
partition_col = table_partition_col_001

#spark.conf.set("fs.azure.sas" + "." + blob_storage + "." + storage_account + "." + url_domain, dbutils.secrets.get(scope = scope ,key = key))

#df, failed_task = load_file(blob_storage, storage_account, url_domain, sr_path_arch)
df, failed_task = load_file(sr_path_arch)

if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")



# COMMAND ----------

df, failed_task = validations_tasks(df, val_max_length, val_control_order)

if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

# COMMAND ----------

df, failed_task = aditional_values(df, sr_proceso, sr_subproceso, sr_subetapa, sr_id_archivo, sr_path_arch, sr_origen_arc)

if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")


# COMMAND ----------

#df.printSchema()

df = df.select("FTC_LINEA", "FTN_NO_LINEA", "FTC_FOLIO", "FTN_ID_ARCHIVO")

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC %reload_ext autoreload
# MAGIC
# MAGIC #habilitar cuando al conexion este lista
# MAGIC #value = '0'
# MAGIC value = write_into_table(conn_name_ora, df, 'append', table_name, conn_options, conn_aditional_options, conn_user, conn_key)
# MAGIC
# MAGIC if value == '0':
# MAGIC     logger.error("Please review log messages")
# MAGIC     notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC     raise Exception("Process ends")
# MAGIC
# MAGIC notification_raised(webhook_url, 1, 'Finish', source, input_parameters)
# MAGIC
# MAGIC #Process ends
