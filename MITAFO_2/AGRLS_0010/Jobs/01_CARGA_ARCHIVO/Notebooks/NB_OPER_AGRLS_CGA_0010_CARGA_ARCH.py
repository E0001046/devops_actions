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
# MAGIC from pyspark.sql.functions import length, lit, monotonically_increasing_id, row_number
# MAGIC from pyspark import StorageLevel
# MAGIC from pyspark.sql.window import Window
# MAGIC
# MAGIC def input_values():
# MAGIC     try:
# MAGIC         dbutils.widgets.text('sr_proceso', '')
# MAGIC         dbutils.widgets.text('sr_subproceso', '')
# MAGIC         dbutils.widgets.text('sr_subetapa', '')
# MAGIC         dbutils.widgets.text('sr_id_archivo', '')
# MAGIC         dbutils.widgets.text('sr_path_arch', '')
# MAGIC         dbutils.widgets.text('sr_origen_arc', '')
# MAGIC
# MAGIC         sr_proceso = dbutils.widgets.get('sr_proceso')
# MAGIC         sr_subproceso = dbutils.widgets.get('sr_subproceso')
# MAGIC         sr_subetapa = dbutils.widgets.get('sr_subetapa')
# MAGIC         sr_id_archivo = dbutils.widgets.get('sr_id_archivo')
# MAGIC         sr_path_arch = dbutils.widgets.get('sr_path_arch')
# MAGIC         sr_origen_arc = dbutils.widgets.get('sr_origen_arc')
# MAGIC
# MAGIC         if any(len(str(value).strip()) == 0 for value in [sr_proceso, sr_subproceso, sr_subetapa, sr_id_archivo, sr_path_arch, sr_origen_arc]):
# MAGIC             logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC             logger.error("Some of the input values are empty or null")
# MAGIC             return '0','0','0','0','0','0','0'
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC         return '0','0','0','0','0','0','0'
# MAGIC     return sr_proceso, sr_subproceso, sr_subetapa, sr_id_archivo, sr_path_arch, sr_origen_arc, '1'
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
# MAGIC         conn_schema_default = config.get(process_name, 'conn_schema_default')
# MAGIC         table_001 = config.get(process_name, 'table_001')
# MAGIC         table_partition_col_001 = config.get(process_name, 'table_partition_col_001')
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC         return '0','0','0','0','0','0'
# MAGIC     return storage_location, conn_schema_default, table_001, table_partition_col_001, '1'
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
# MAGIC         #@Joan: Cambio location por storage_location
# MAGIC         #dbutils.fs.ls(storage_location + sr_path_arch)
# MAGIC         #schema = StructType([StructField('FTC_LINEA', StringType(), True)])
# MAGIC         #full_connection = "wasbs" + "://" + blob_storage + "@" + storage_account + "." + url_domain + "/"
# MAGIC         #full_connection = f"wasbs://{blob_storage}@{storage_account}.{url_domain}/"
# MAGIC         #full_connection = '/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/'
# MAGIC         #df = spark.read.csv(storage_location + sr_path_arch, schema = schema)
# MAGIC         schema = StructType([StructField('FTC_LINEA', StringType(), True)])
# MAGIC         #df = spark.read.csv(storage_location + sr_path_arch, header=False, inferSchema=True)
# MAGIC         df = spark.read.text(storage_location + sr_path_arch)
# MAGIC         df = df.withColumnRenamed("value", "FTC_LINEA")
# MAGIC         #display(df)
# MAGIC         indexed_df = df.withColumn("idx", monotonically_increasing_id() + 1)
# MAGIC         # Create the window specification
# MAGIC         w = Window.orderBy("idx")
# MAGIC         # Use row number with the window specification
# MAGIC         indexed_df = indexed_df.withColumn("FTN_NO_LINEA", row_number().over(w))
# MAGIC
# MAGIC         indexed_df = indexed_df.withColumnRenamed("_c0", "FTC_LINEA")
# MAGIC         # Drop the created increasing data column
# MAGIC         indexed_df = indexed_df.drop("idx")
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC         return '0', '0'
# MAGIC     return indexed_df, '1'
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
# MAGIC         df = df.withColumn('FTN_ID_ARCHIVO', lit(sr_id_archivo)) \
# MAGIC                 .withColumn('FTC_FOLIO', lit(''))
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC         return '0', '0'
# MAGIC     return df, '1'
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
# MAGIC
# MAGIC     sr_proceso, sr_subproceso, sr_subetapa, sr_id_archivo, sr_path_arch, sr_origen_arc, failed_task = input_values()
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         raise Exception("An error ocurred, check out log messages")
# MAGIC     
# MAGIC     input_parameters = dbutils.widgets.getAll().items()
# MAGIC
# MAGIC     process_name = 'root'
# MAGIC     
# MAGIC     webhook_url, channel, failed_task = conf_init_values(config_file, process_name, 'CARG_ARCH')
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     conn_name_ora = 'jdbc_oracle'
# MAGIC     conn_options, conn_aditional_options, conn_user, conn_key, conn_url, scope,failed_task = conf_conn_values(config_conn_file, conn_name_ora)
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC     
# MAGIC     config_process_file = root_repo + '/' + 'AGRLS_0010/Jobs/01_CARGA_ARCHIVO/Conf/CF_PART_PROC.py.properties'
# MAGIC     
# MAGIC     process_name = 'root'
# MAGIC     
# MAGIC     storage_location, conn_schema_default, table_001, table_partition_col_001, failed_task = conf_process_values(config_process_file, process_name)
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC     
# MAGIC     #empty_df = sqlContext.createDataFrame(sc.emptyRDD(), schema = StructType([]))

# COMMAND ----------

#%load_ext autoreload
#%autoreload 2
table_name = conn_schema_default + '.' + table_001
partition_col = table_partition_col_001

#spark.conf.set("fs.azure.sas" + "." + blob_storage + "." + storage_account + "." + url_domain, dbutils.secrets.get(scope = scope ,key = key))

#df, failed_task = load_file(blob_storage, storage_account, url_domain, sr_path_arch)
df, failed_task = load_file(storage_location, sr_path_arch)

df.persist(StorageLevel.DISK_ONLY)

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

#pdf = df.toPandas()
#pdf = pdf.assign(FTN_NO_LINEA=range(1, len(pdf) + 1))

#df.unpersist()

#df = spark.createDataFrame(pdf)

#del pdf

df = df.select("FTC_LINEA", "FTN_NO_LINEA", "FTC_FOLIO", "FTN_ID_ARCHIVO")

# COMMAND ----------

#habilitar cuando al conexion este lista
#value = '0'
df.persist(StorageLevel.DISK_ONLY)

print(table_name)

value = write_into_table(conn_name_ora, df, 'append', table_name, conn_options, conn_aditional_options, conn_user, conn_key)

if value == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

df.unpersist()

notification_raised(webhook_url, 0, 'DONE', source, input_parameters)

#Process ends
