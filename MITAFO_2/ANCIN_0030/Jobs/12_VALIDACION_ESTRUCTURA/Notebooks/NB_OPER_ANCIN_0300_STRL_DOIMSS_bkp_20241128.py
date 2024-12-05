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
# MAGIC from pyspark.sql.functions import length, lit, current_timestamp, concat, col, when, lead, monotonically_increasing_id, from_utc_timestamp, to_timestamp, row_number
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
# MAGIC                 .withColumn('FECHA_CARGA', to_timestamp(from_utc_timestamp(current_timestamp(), "GMT-6"))) \
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
# MAGIC         
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
# MAGIC     conn_options, conn_aditional_options, conn_user, conn_key, conn_url, scope, failed_task = conf_conn_values(config_conn_file, conn_name_ora)
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
# MAGIC     debug = True
# MAGIC
# MAGIC     #empty_df = sqlContext.createDataFrame(sc.emptyRDD(), schema = StructType([]))

# COMMAND ----------

with open(sql_conf_file) as f:
    file_config_sql = json.load(f)

conf_values = [ (fields['step_id'], '\n'.join(fields['value'])) for line, value in file_config_sql.items() if line == 'steps' for fields in value ]


# COMMAND ----------

#dbutils.fs.head(storage_location + sr_path_arch)

# COMMAND ----------

#Carga el archivo a un dataframe
table_name = conn_schema_default + '.' + table_001

dfLoaded, failed_task = load_file(storage_location, sr_path_arch)

if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

#df.persist(StorageLevel.DISK_ONLY)

if debug:
    display(dfLoaded)

# COMMAND ----------

#Building data
dfLoaded.persist(StorageLevel.DISK_ONLY)

df, failed_task = aditional_values(dfLoaded, sr_id_archivo, sr_subetapa)

if debug:
    display(df)

if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

dfLoaded.unpersist()

df.repartition(20)

df.persist(StorageLevel.DISK_ONLY)

# COMMAND ----------

dfVal, failed_task = validation_task(df)
dfVal_2 = dfVal.filter(dfVal.FTN_NO_LINEA != 1)

dfVal = dfVal.repartition(20)
dfVal_2 = dfVal_2.repartition(20)

df.unpersist()

if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

temp_view = "TEMP_VALIDACION_ESTRUCTURA_ARCHIVO" # original
temp_view_2 = "TEMP_VALIDACION_ESTRUCTURA_ARCHIVO_2" # para validar (df_output_001)

dfVal.persist(StorageLevel.DISK_ONLY)
dfVal_2.persist(StorageLevel.DISK_ONLY)

try:
    dfVal.createOrReplaceTempView(temp_view) # original
except Exception as e:
    logger.error("An error was raised: " + str(e))
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

if debug:
    display(spark.sql("SELECT * FROM TEMP_VALIDACION_ESTRUCTURA_ARCHIVO"))

# COMMAND ----------

from pyspark.sql.functions import col, lit, when, concat

df_test = dfVal_2.filter(dfVal_2.VAL_ESTATUS == 0).select(
    col("SIG_CONTROL").alias("FTC_CONTROL"),
    "VAL_ESTATUS",
    "SR_ID_ARCHIVO",
    "FECHA_CARGA",
    col("SIG_LINEA").alias("VALOR_CAMPO"),
    col("FTN_NO_LINEA").alias("LINEA_TEMP"),
)
df_test = df_test.withColumn("FTN_NO_LINEA", col("LINEA_TEMP") + lit(1))

df_test = df_test.withColumn(
    "DESC_ERROR",
    when(
        (col("FTC_CONTROL") == "02") | (col("FTC_CONTROL") == "08"),
        "Error de orden de los detalles incorrecto",
    ).otherwise(concat(lit("Error en tipo de registro: "), col("FTC_CONTROL"))),
)

df_test.createOrReplaceTempView(temp_view_2)

if debug:
    display(df_test)

# COMMAND ----------

# DBTITLE 1,test
from pyspark.sql.functions import row_number

statement = f"""
SELECT
    SR_ID_ARCHIVO FTN_ID_ARCHIVO
    ,FECHA_CARGA
    ,CASE WHEN LENGTH(TRIM(FTC_CONTROL)) = 0 THEN '00' ELSE FTC_CONTROL END FTC_CONTROL
    ,FTN_NO_LINEA
    ,'Tipo de registro' as CAMPO
    ,'Error en tipo de registro' as VALIDACION
    ,VALOR_CAMPO
    ,0 as COD_ERROR
    ,DESC_ERROR
    ,'N/A' as VALOR_A_VALIDAR
FROM {temp_view_2}
WHERE VAL_ESTATUS = 0
ORDER BY FTN_NO_LINEA ASC
"""

df_output_001 = spark.sql(statement)

df_output_001.persist(StorageLevel.DISK_ONLY)

if debug:
    display(df_output_001)

# COMMAND ----------

# query_statement = '001'

# params = [temp_view]

# statement, failed_task = getting_statement(conf_values, query_statement, params)

# print(statement)

# if failed_task == '0':
#     logger.error("No value %s found", statement)
#     logger.error("Please review log messages")
#     notification_raised(webhook_url, -1, message, source, input_parameters)
#     raise Exception("Process ends")

# df_output_001 = spark.sql(statement)

# df_output_001.persist(StorageLevel.DISK_ONLY)

# if debug:
#     display(df_output_001)

# COMMAND ----------

query_statement = '002'

params = [temp_view]

statement, failed_task = getting_statement(conf_values, query_statement, params)

print(statement)

if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

df_output_002 = spark.sql(statement)

df_output_002.persist(StorageLevel.DISK_ONLY)

if debug:
    display(df_output_002)

# COMMAND ----------

query_statement = '003'

params = [temp_view]

statement, failed_task = getting_statement(conf_values, query_statement, params)

print(statement)

if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

df_output_003 = spark.sql(statement)

df_output_003.persist(StorageLevel.DISK_ONLY)

if debug:
    display(df_output_003)

# COMMAND ----------

query_statement = '004'

params = [temp_view]

statement, failed_task = getting_statement(conf_values, query_statement, params)

print(statement)

if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

df_output_004 = spark.sql(statement)

df_output_004.persist(StorageLevel.DISK_ONLY)

if debug:
    display(df_output_004)

# COMMAND ----------

full_file_name = (
    storage_location + err_repo_path + "/" + sr_id_archivo + "_" + output_file_name_001
)

file_name = full_file_name + "_TEMP"

try:
    df_output_001.write.format("csv").mode("overwrite").option("header", header).save(
        full_file_name + "_TEMP"
    )
    dataframe = spark.read.option("header", header).csv(full_file_name + "_TEMP")
    dataframe.coalesce(1).write.format("csv").mode("overwrite").option(
        "header", header
    ).save(full_file_name)
except Exception as e:
    logger.error("An error was raised: " + str(e))
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

if debug:
    display(dataframe)

# COMMAND ----------

try:
    df_output_002.write.format("csv").mode('overwrite').option("header", header).save(full_file_name + '_TEMP')
    dataframe = spark.read.option('header', header).csv(full_file_name + '_TEMP')
    dataframe.coalesce(1).write.format('csv').mode('append').option('header', False).save(full_file_name)
except Exception as e:
    logger.error("An error was raised: " + str(e))
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

# COMMAND ----------

try:
    df_output_003.write.format("csv").mode('overwrite').option("header", header).save(full_file_name + '_TEMP')
    dataframe = spark.read.option('header', header).csv(full_file_name + '_TEMP')
    dataframe.coalesce(1).write.format('csv').mode('append').option('header', False).save(full_file_name)
except Exception as e:
    logger.error("An error was raised: " + str(e))
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

# COMMAND ----------

try:
    df_output_004.write.format("csv").mode('overwrite').option("header", header).save(full_file_name + '_TEMP')
    dataframe = spark.read.option('header', header).csv(full_file_name + '_TEMP')
    dataframe.coalesce(1).write.format('csv').mode('append').option('header', False).save(full_file_name)
except Exception as e:
    logger.error("An error was raised: " + str(e))
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

# COMMAND ----------

try:
    dfFile = spark.read.option('header', False).csv(full_file_name).sort(1, ascending = False)
    df_output_001.unpersist()
    df_output_002.unpersist()
    df_output_003.unpersist()
    df_output_004.unpersist()
    dfFile.coalesce(1).write.format('csv').mode('overwrite').option('header', False).save(full_file_name)
except Exception as e:
    logger.error("An error was raised: " + str(e))
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

# COMMAND ----------

try:
    full_file_name_aux = full_file_name + '_TEMP'
    dbutils.fs.rm(full_file_name_aux, recurse = True)

    file_name_tmp = dbutils.fs.ls(full_file_name)
    file_name_new = list(filter(lambda x : x[0].endswith('csv'), file_name_tmp))

    dbutils.fs.cp(file_name_new[0][0], full_file_name_aux)
    dbutils.fs.rm(full_file_name, recurse=True)
    dbutils.fs.cp(full_file_name_aux, full_file_name)
    dbutils.fs.rm(full_file_name_aux, recurse = True)
except Exception as e:
    logger.error("An error was raised: " + str(e))
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

# COMMAND ----------

lines = dbutils.fs.head(full_file_name)

if (lines.count('\n') == 1 and header == 'True') or (lines.count('\n') == 0):
    dbutils.fs.rm(full_file_name, recurse = True)

# COMMAND ----------

target_table = conn_schema_default + '.' + table_001

logger.info("Target table: %s", target_table)

query_statement = '005'

params = [temp_view]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

# COMMAND ----------

mode = 'APPEND'

df_output_table = spark.sql(statement)

df_output_table.repartition(20)

failed_task = write_into_table(conn_name_ora, df_output_table, mode, target_table, conn_options, conn_aditional_options, conn_user, conn_key)

if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")


notification_raised(webhook_url, 0, "DONE", source, input_parameters)
