# Databricks notebook source
# DBTITLE 1,Set values & main function
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
# MAGIC         #Input var values from widgets
# MAGIC         dbutils.widgets.text('sr_folio', '')
# MAGIC         dbutils.widgets.text('sr_proceso', '')
# MAGIC         dbutils.widgets.text('sr_subproceso', '')
# MAGIC         dbutils.widgets.text('sr_origen_arc', '')
# MAGIC         dbutils.widgets.text('sr_dt_org_arc', '')
# MAGIC         dbutils.widgets.text('sr_subetapa', '')
# MAGIC         dbutils.widgets.text('sr_sec_lote', '')
# MAGIC         dbutils.widgets.text('sr_fecha_lote', '')
# MAGIC         dbutils.widgets.text('sr_fecha_acc', '')
# MAGIC         dbutils.widgets.text('sr_tipo_archivo', '')
# MAGIC         dbutils.widgets.text('sr_estatus_mov', '')
# MAGIC         dbutils.widgets.text('sr_tipo_mov', '')
# MAGIC         dbutils.widgets.text('sr_accion', '')
# MAGIC         dbutils.widgets.text('sr_usuario', '')
# MAGIC         dbutils.widgets.text('sr_etapa', '')
# MAGIC         dbutils.widgets.text('sr_id_snapshot', '')
# MAGIC         dbutils.widgets.text('sr_instancia_proceso', '')
# MAGIC
# MAGIC         sr_folio = dbutils.widgets.get('sr_folio')
# MAGIC         sr_proceso = dbutils.widgets.get('sr_proceso')
# MAGIC         sr_subproceso = dbutils.widgets.get('sr_subproceso')
# MAGIC         sr_origen_arc = dbutils.widgets.get('sr_origen_arc')
# MAGIC         sr_dt_org_arc = dbutils.widgets.get('sr_dt_org_arc')
# MAGIC         sr_subetapa = dbutils.widgets.get('sr_subetapa')
# MAGIC         sr_sec_lote = dbutils.widgets.get('sr_sec_lote')
# MAGIC         sr_fecha_lote = dbutils.widgets.get('sr_fecha_lote')
# MAGIC         sr_fecha_acc = dbutils.widgets.get('sr_fecha_acc')
# MAGIC         sr_tipo_archivo = dbutils.widgets.get('sr_tipo_archivo')
# MAGIC         sr_estatus_mov = dbutils.widgets.get('sr_estatus_mov')
# MAGIC         sr_tipo_mov = dbutils.widgets.get('sr_tipo_mov')
# MAGIC         sr_accion = dbutils.widgets.get('sr_accion')
# MAGIC         sr_usuario = dbutils.widgets.get('sr_usuario')
# MAGIC         sr_etapa = dbutils.widgets.get('sr_etapa')
# MAGIC         sr_id_snapshot = dbutils.widgets.get('sr_id_snapshot')
# MAGIC         sr_instancia_proceso = dbutils.widgets.get('sr_instancia_proceso')
# MAGIC
# MAGIC         if any(len(str(value).strip()) == 0 for value in [sr_folio,sr_proceso,sr_subproceso,sr_origen_arc,sr_dt_org_arc,sr_subetapa,sr_sec_lote,sr_fecha_lote,sr_fecha_acc,sr_tipo_archivo,sr_estatus_mov,sr_tipo_mov,sr_accion,sr_usuario,sr_etapa,sr_id_snapshot,sr_instancia_proceso]):    
# MAGIC             logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC             logger.error("Some of the input values are empty or null")
# MAGIC             return '0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0'
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: %s", str(e))
# MAGIC         return '0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0'
# MAGIC     return sr_folio,sr_proceso,sr_subproceso,sr_origen_arc,sr_dt_org_arc,sr_subetapa,sr_sec_lote,sr_fecha_lote,sr_fecha_acc,sr_tipo_archivo,sr_estatus_mov,sr_tipo_mov,sr_accion,sr_usuario,sr_etapa,sr_id_snapshot,sr_instancia_proceso,'1'
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
# MAGIC         #Please review de Conf file and verify all schemas, tables & views
# MAGIC         sql_conf_file = config.get(process_name, 'sql_conf_file')
# MAGIC         conn_schema_001 = config.get(process_name, 'conn_schema_001')
# MAGIC         conn_schema_002 = config.get(process_name,'conn_schema_002')
# MAGIC         table_001 = config.get(process_name, 'table_001')
# MAGIC         table_002 = config.get(process_name, 'table_002')
# MAGIC       
# MAGIC         external_location = config.get(process_name, 'external_location')
# MAGIC         err_repo_path = config.get(process_name, 'err_repo_path')
# MAGIC         #sep = config.get(process_name, 'sep')
# MAGIC         header = config.get(process_name, 'header')
# MAGIC     
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC         return '0','0','0','0','0','0','0','0','0'
# MAGIC     return sql_conf_file, conn_schema_001, conn_schema_002,table_001, table_002,external_location,err_repo_path,header,'1'
# MAGIC
# MAGIC
# MAGIC
# MAGIC #Main function
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
# MAGIC     #Get the values from the workspace paths
# MAGIC     config_file = root_repo + '/' + 'CGRLS_0010/Conf/CF_GRLS_PROCESS.py.properties'
# MAGIC     config_conn_file = root_repo + '/' + 'CGRLS_0010/Conf/CF_GRLS_CONN.py.properties'
# MAGIC     config_process_file = root_repo + '/' + 'ANCIN_0030/Jobs/05_CONCLUSION/01_GENERA_ARCHIVOS/DISPERSIONES/Conf/CF_PART_PROC.py.properties'
# MAGIC
# MAGIC     sr_folio,sr_proceso,sr_subproceso,sr_origen_arc,sr_dt_org_arc,sr_subetapa,sr_sec_lote,sr_fecha_lote,sr_fecha_acc,sr_tipo_archivo,sr_estatus_mov,sr_tipo_mov,sr_accion,sr_usuario,sr_etapa,sr_id_snapshot,sr_instancia_proceso,failed_task= input_values()
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         #notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("An error ocurred, check out log messages")
# MAGIC     
# MAGIC     input_parameters = dbutils.widgets.getAll().items()
# MAGIC     
# MAGIC     process_name = 'root'
# MAGIC     webhook_url, channel, failed_task = conf_init_values(config_file, process_name, 'VAL_SUA')
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     process_name = 'root'
# MAGIC     sql_conf_file,conn_schema_001,conn_schema_002,table_001,table_002,external_location,err_repo_path,header,failed_task = conf_process_values(config_process_file, process_name)
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
# MAGIC     #Json file path
# MAGIC     sql_conf_file = root_repo + '/' + 'ANCIN_0030/Jobs/05_CONCLUSION/01_GENERA_ARCHIVOS/DISPERSIONES/JSON/' + sql_conf_file
# MAGIC

# COMMAND ----------

# DBTITLE 1,Extraction from JSON file
#Read Json file & extract the values 
with open(sql_conf_file) as f:
    file_config_sql = json.load(f)

conf_values = [ (fields['step_id'], '\n'.join(fields['value'])) for line, value in file_config_sql.items() if line == 'steps' for fields in value ]

# COMMAND ----------

# DBTITLE 1,Query 001 configuration
#Table definition 001 (CIERREN_ETL.TTSISGRAL_ETL_MOVIMIENTOS)
#Table definition 002 (CIERREN.TNAFORECA_SUA)

table_name_001 = conn_schema_001 + '.' + table_001
table_name_002 = conn_schema_002 + '.' + table_002
query_statement = '001'

params = [sr_folio, table_name_001, table_name_002 ]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

# COMMAND ----------

# DBTITLE 1,Statement
#print(statement)

# COMMAND ----------

# DBTITLE 1,Query execution
#Query 001 execution
df, failed_task = query_table(conn_name_ora, spark, statement,  conn_options, conn_user, conn_key)

# COMMAND ----------

# DBTITLE 1,Display Data Frame
#display(df)

# COMMAND ----------

# DBTITLE 1,Temporary view generation
#Temp view 001 generation (TEMP_DOIMSS_GEN_SUA_01_sr_folio)
temp_view = 'TEMP_DOIMSS_GEN_SUA_01' + '_' + sr_folio
df.createOrReplaceTempView(temp_view) 


# COMMAND ----------

# DBTITLE 1,Query 008 configuration

#Query 008 using TEMP_DOIMSS_GEN_SUA_01_sr_folio
query_statement = '008'

params = [temp_view]

statement, failed_task = getting_statement(conf_values, query_statement, params)

if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

# COMMAND ----------

# DBTITLE 1,Statement
#print(statement)

# COMMAND ----------

# DBTITLE 1,Query execution by spark and count DF
df = spark.sql(statement)
#df.count()

# COMMAND ----------

# DBTITLE 1,Display DF
#display(df)

# COMMAND ----------

# DBTITLE 1,Concat function to generate a view
from pyspark.sql.functions import concat_ws, lit, trim, col,when

df = df.withColumn(
    "ARCHIVO_SUA",
    concat_ws(
        '',
        #df["DISP_TIPREG_SUA"],
        when(trim(col("DISP_TIPREG_SUA")) == "", lit("  ")).otherwise(
            col("DISP_TIPREG_SUA")
        ),    
        df["DISP_NUMCUE"],
        df["DISP_NSSTRA"],
        df["DISP_CORREL"],
        df["DISP_RFCTRA"],
        df["DISP_CURP"],
        df["DISP_NOMAFI"],
        df["DISP_PERPAG"],
        df["DISP_FECPAG"],
        df["DISP_FECCRCV"],
        df["DISP_FECVGUB"],
        df["DISP_FECVVIV"],
        df["DISP_SALBAS"],
        df["DISP_FOLSUA"],
        df["DISP_NSSEMP"],
        df["DISP_RFCEMP"],
        df["DISP_CLAREC"],
        df["DISP_DIACOT"],
        df["DISP_INCAPA"],
        df["DISP_AUSENT"],
        df["DISP_MONRET"],
        df["DISP_ACTRET"],
        df["DISP_COMRET"],
        df["DISP_MONCVE"],
        df["DISP_ACTCVE"],
        df["DISP_COMCVE"],
        df["DISP_MONVOL"],
        df["DISP_MONVIV"],
        df["DISP_CUOSOC"],
        df["DISP_ACTCSO"],
        df["DISP_APOEST"],
        df["DISP_ACTEST"],
        df["DISP_COMEST"],
        df["DISP_APOESP"],
        df["DISP_ACTESP"],
        df["DISP_COMESP"],
        df["DISP_FECLIQ_RCV"],
        df["DISP_FECLIQ_GUB"],
        df["DISP_FECTRA"],
        df["DISP_SECLOT"],
        df["DISP_AFORE"],
        df["DISP_DEVPAG"],
        df["DISP_ALP"],
        df["DISP_ASA"],
        df["DISP_REMVIV"],
        df["DISP_APINTREM"],
        df["DISP_INTGENEXTVIV"],
        df["DISP_APINTVIVEXT"],
        df["DISP_ACR"],
    ),
)
df_final = df.select("ARCHIVO_SUA")

# COMMAND ----------

# DBTITLE 1,Display Data Frame final
#display(df_final)
#df.count()

# COMMAND ----------

# DBTITLE 1,Time, path & Data Frame values
from datetime import datetime

# Generate actual date in format YYYYMMDD
fecha_actual = datetime.now().strftime("%Y%m%d")

# Create file name
full_file_name = external_location + err_repo_path + "/SUA_" + fecha_actual + "_" + sr_sec_lote + ".DAT"

try:
    # Save in temporary Data Frame
    df_final.write.format("csv").mode("overwrite").option("header", header).option("lineSep","\r\n").save(
        full_file_name + "_TEMP"
    )
    dataframe = spark.read.option("header", header).csv(full_file_name + "_TEMP")
    dataframe.coalesce(1).write.format("csv").mode("overwrite").option(
        "header", header
    ).option("lineSep","\r\n").save(full_file_name)
except Exception as e:
    logger.error("An error was raised: " + str(e))
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

# COMMAND ----------

# DBTITLE 1,Overwrite a CSV file
try:
    dfFile = spark.read.option('header', header).csv(full_file_name)
    dfFile.coalesce(1).write.format('csv').mode('overwrite').option('header', header).option("lineSep","\r\n").save(full_file_name)
except Exception as e:
    logger.error("An error was raised: " + str(e))
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

# COMMAND ----------

# DBTITLE 1,Set operations to  manage distributed files using dbutils
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

notification_raised(webhook_url, 0, 'DONE', source, input_parameters)

# COMMAND ----------

# DBTITLE 1,Copy file from ADLS to Databricks file system
#Copy file from ADLS to Databricks file system  "/SUA_" + fecha_actual + "_" + sr_sec_lote + ".DAT"
#dbutils.fs.cp('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/INTEGRITY/SUA_' + fecha_actual + '_' + sr_sec_lote + '.DAT', 
#'/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/SUA_' + fecha_actual + '_' + sr_sec_lote + '.DAT')

# COMMAND ----------

#dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/INTEGRITY/')
