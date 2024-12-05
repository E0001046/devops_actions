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
# MAGIC         # Unity
# MAGIC         debug = config.get(process_name, "debug")
# MAGIC         debug = debug.lower() == "true"
# MAGIC         catalog_name = config.get(process_name, "catalog_name")
# MAGIC         schema_name = config.get(process_name, "schema_name")
# MAGIC
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
# MAGIC         )
# MAGIC     return (
# MAGIC         sql_conf_file,
# MAGIC         # unity
# MAGIC         debug,
# MAGIC         catalog_name,
# MAGIC         schema_name,
# MAGIC         "1",
# MAGIC     )
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
# MAGIC         + "/"
# MAGIC         + "/ANCIN_0030/Jobs/01_IDENTIFICACION_DE_CLIENTE/01_DISPERSIONES/JSON/"
# MAGIC         + sql_conf_file
# MAGIC     )

# COMMAND ----------

with open(sql_conf_file) as f:
    file_config_sql = json.load(f)

conf_values = [ (fields['step_id'], '\n'.join(fields['value'])) for line, value in file_config_sql.items() if line == 'steps' for fields in value ]

# COMMAND ----------

# query_statement = '019'
# catalog_schema = f"{catalog_name}.{schema_name}"

# params = [sr_id_archivo, catalog_schema]

# statement, failed_task = getting_statement(conf_values, query_statement, params)
# print(statement)

# if failed_task == '0':
#     logger.error("No value %s found", statement)
#     logger.error("Please review log messages")
#     notification_raised(webhook_url, -1, message, source, input_parameters)
#     raise Exception("Process ends")

# df=spark.sql(statement)
  
# if debug:
#     display(df)    


# COMMAND ----------

statement = f""" WITH CARGA_CLIENTE
AS (
   SELECT ENCON.FTN_NSS_CURP
        ,ENCON.FTC_IDENTIFICADOS
        ,ENCON.FTN_NUM_CTA_INVDUAL
        ,ENCON.FTN_ID_DIAGNOSTICO
        ,ENCON.FTN_ID_SUBP_NO_CONV
        ,ENCON.FTC_NOMBRE_BUC
        ,ENCON.FTC_AP_PATERNO_BUC
        ,ENCON.FTC_AP_MATERNO_BUC
        ,ENCON.FTC_RFC_BUC
        ,ENCON.FTC_FOLIO
        ,ENCON.FTN_ID_ARCHIVO
        ,ENCON.FTN_NSS
        ,ENCON.FTC_CURP
        ,ENCON.FTC_RFC
        ,ENCON.FTC_NOMBRE_CTE
        ,ENCON.FTC_CLAVE_ENT_RECEP
        ,ENCON.FTN_VIGENCIA
        ,ENCON.MARC_DUP
        ,ENCON.FCN_ID_TIPO_SUBCTA
        ,FECHA.FCC_VALOR_IND AS FTD_FECHA_CERTIFICACION
        ,CASE WHEN PENSION.FCC_VALOR_IND IS NULL THEN '0' ELSE '1' END FTN_CTE_PENSIONADO
    FROM {catalog_name}.{schema_name}.TEMP_ENCONTRADOS_06_{sr_id_archivo} ENCON
    LEFT JOIN {catalog_name}.{schema_name}.TEMP_FECHAAPCTAIND_05_{sr_id_archivo} FECHA
        ON ENCON.FTN_NUM_CTA_INVDUAL = FECHA.FTN_NUM_CTA_INVDUAL
    LEFT JOIN {catalog_name}.{schema_name}.TEMP_PENSIONADO_11_{sr_id_archivo} PENSION
        ON ENCON.FTN_NUM_CTA_INVDUAL = PENSION.FTN_NUM_CTA_INVDUAL

        UNION
        
    SELECT NOENCON.FTN_NSS_CURP
        ,'0' AS FTC_IDENTIFICADOS
        ,NOENCON.FTN_NUM_CTA_INVDUAL
        ,NOENCON.FTN_ID_DIAGNOSTICO
        ,NULL AS FTN_ID_SUBP_NO_CONV
        ,NOENCON.FTC_NOMBRE_BUC
        ,NOENCON.FTC_AP_PATERNO_BUC
        ,NOENCON.FTC_AP_MATERNO_BUC
        ,NOENCON.FTC_RFC_BUC
        ,NOENCON.FTC_FOLIO
        ,NOENCON.FTN_ID_ARCHIVO
        ,NOENCON.FTN_NSS
        ,NOENCON.FTC_CURP
        ,NOENCON.FTC_RFC
        ,NOENCON.FTC_NOMBRE_CTE
        ,NOENCON.FTC_CLAVE_ENT_RECEP
        ,NOENCON.FTN_VIGENCIA
        ,NULL AS MARC_DUP
        ,NOENCON.FCN_ID_TIPO_SUBCTA
        ,NULL AS FTD_FECHA_CERTIFICACION
        ,NULL AS FTN_CTE_PENSIONADO
    FROM {catalog_name}.{schema_name}.TEMP_NOENCONTRADOS_{sr_id_archivo} NOENCON
 )
 SELECT FTN_NSS_CURP
        ,FTC_IDENTIFICADOS
        ,FTN_NUM_CTA_INVDUAL
        ,FTN_ID_DIAGNOSTICO
        ,FTN_ID_SUBP_NO_CONV
        ,FTC_NOMBRE_BUC
        ,FTC_AP_PATERNO_BUC
        ,FTC_AP_MATERNO_BUC
        ,FTC_RFC_BUC
        ,FTC_FOLIO
        ,FTN_ID_ARCHIVO
        ,FTN_NSS
        ,FTC_CURP
        ,FTC_RFC
        ,FTC_NOMBRE_CTE
        ,FTC_CLAVE_ENT_RECEP
        ,FTN_VIGENCIA
        ,MARC_DUP
        ,FCN_ID_TIPO_SUBCTA
        ,FTD_FECHA_CERTIFICACION
        ,FTN_CTE_PENSIONADO
    FROM CARGA_CLIENTE
    WHERE MARC_DUP <> 'D'
        OR MARC_DUP = 'D'
        OR MARC_DUP IS NULL

"""

df = spark.sql(statement)
if debug:
    display(df)

# COMMAND ----------

# DBTITLE 1,mejora
from pyspark.sql import Window
import pyspark.sql.functions as F

window_spec = Window.partitionBy("FTN_NSS_CURP").orderBy(
    F.col("FTN_VIGENCIA").desc(), F.col("FTD_FECHA_CERTIFICACION").desc()
)

df_ordenado = df.withColumn("row_number", F.row_number().over(window_spec)).filter(F.col("row_number") == 1).drop("row_number")

if debug:
    display(df_ordenado)

# COMMAND ----------

# DBTITLE 1,original
# df_ordenado = df.orderBy(
#     ["FTN_NSS_CURP", "FTN_VIGENCIA", "FTD_FECHA_CERTIFICACION",],
#     ascending=[False, False, False],
# ).dropDuplicates(["FTN_NSS_CURP"])
# if debug:
#     display(df_ordenado)

# COMMAND ----------

# DBTITLE 1,CREACIÓN DE VISTA GLOBAL IDENTIFICACIÓN CLIENTES
temp_view = 'TEMP_IDENTIFICACIONCLIENTES_07' + '_' + sr_id_archivo
df_ordenado.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog_name}.{schema_name}.{temp_view}"
)

# spark.catalog.dropGlobalTempView(temp_view)
# df.createOrReplaceGlobalTempView(temp_view)

#spark.sql(f"select * from global_temp.TEMP_TRAMITE_01_{sr_id_archivo}").show()
#spark.sql(f"select * from global_temp.TEMP_TRAMITE_01_{sr_id_archivo}").count()

# COMMAND ----------

'''%sql
select * from global_temp.temp_encontrados_06_29288'''



# COMMAND ----------

'''%sql
select * from global_temp.temp_fechaapctaind_05_29288'''

# COMMAND ----------

'''%sql select * from global_temp.TEMP_Pensionado_08_29288'''

# COMMAND ----------

# Liberar la caché del DataFrame
df.unpersist()

# Si ya no necesitas df en lo absoluto, puedes eliminarlo
del df, df_ordenado

# Forzar la recolección de basura para liberar memoria
import gc
gc.collect()
