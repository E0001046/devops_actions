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
# MAGIC         var_tramite = config.get(process_name, "var_tramite")
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
# MAGIC             "0",
# MAGIC         )
# MAGIC     return (
# MAGIC         sql_conf_file,
# MAGIC         var_tramite,
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
# MAGIC         var_tramite,
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

if var_tramite == 'DOIMSS':
    p_ind = '3'
elif var_tramite == 'DOISSSTE':
    p_ind = '2' 
else: 'error'

# query_statement = '018'

# params = [sr_folio,p_ind]

# statement, failed_task = getting_statement(conf_values, query_statement, params)

# if failed_task == '0':
#     logger.error("No value %s found", statement)
#     logger.error("Please review log messages")
#     notification_raised(webhook_url, -1, message, source, input_parameters)
#     raise Exception("Process ends")

# df, failed_task = query_table(conn_name_ora, spark, statement,  conn_options, conn_user, conn_key)
# df.cache()

# if failed_task == '0':
#     logger.error("Please review log messages")
#     notification_raised(webhook_url, -1, message, source, input_parameters)
#     raise Exception("An error raised")

# if debug:
#     display(df)    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Primera parte

# COMMAND ----------

statement = f"""
SELECT  
    a.FTN_NSS_CURP,
    a.FTN_NUM_CTA_INVDUAL
FROM 
    (SELECT  
        CASE WHEN 3={p_ind} THEN FTN_NSS ELSE (CASE WHEN 2={p_ind} THEN FTC_CURP ELSE '0' END) END AS FTN_NSS_CURP,
        FTN_NUM_CTA_INVDUAL
    FROM CIERREN.TTAFOGRAL_CTA_INVDUAL) a
INNER JOIN 
    (SELECT DISTINCT FTC_NSS_CURP 
     FROM CIERREN_ETL.TTSISGRAL_ETL_DISPERSION_CTAS 
     WHERE FTC_FOLIO= '{sr_folio}') b
ON a.FTN_NSS_CURP = b.FTC_NSS_CURP
"""
print(statement)

# COMMAND ----------

df_A, failed_task = query_table(conn_name_ora, spark, statement,  conn_options, conn_user, conn_key)
if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")


if debug:
    display(df_A)

temp_view = f"temp_df_a_{sr_id_archivo}"
df_A.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog_name}.{schema_name}.{temp_view}"
)
del df_A

# COMMAND ----------

# MAGIC %md
# MAGIC ### Segunda parte

# COMMAND ----------

statement = f"""
SELECT  
    FCN_ID_TIPO_SUBCTA,
    FTC_FOLIO,
    CASE WHEN 3={p_ind} THEN FTN_NSS ELSE (CASE WHEN 2={p_ind} THEN FTC_CURP ELSE '0' END) END AS FTN_NSS_CURP
FROM CIERREN_ETL.TTSISGRAL_ETL_PRE_DISPERSION a
WHERE FTC_FOLIO='{sr_folio}'
"""

# COMMAND ----------

df_B, failed_task = query_table(conn_name_ora, spark, statement,  conn_options, conn_user, conn_key)
if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")

if debug:
    display(df_B)

temp_view = f"temp_df_b_{sr_id_archivo}"
df_B.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog_name}.{schema_name}.{temp_view}"
)
del df_B

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tercera parte

# COMMAND ----------

statement = f"""
SELECT 
    subc.FCN_ID_CAT_SUBCTA,
    subc.FCN_ID_GRUPO,
    subc.FCN_ID_TIPO_SUBCTA
FROM CIERREN.TCCRXGRAL_TIPO_SUBCTA subc
INNER JOIN CIERREN.TCCRXGRAL_CAT_CATALOGO cat ON subc.FCN_ID_REGIMEN = cat.FCN_ID_CAT_CATALOGO
INNER JOIN CIERREN.TCCRXGRAL_TIPO_CAT tc ON tc.FCN_ID_TIPO_CAT = cat.FCN_ID_TIPO_CAT
WHERE tc.FCN_ID_TIPO_CAT = 21
AND tc.FCB_VIGENCIA = 1
AND cat.FCB_VIGENCIA = 1
"""

# COMMAND ----------

df_C, failed_task = query_table(conn_name_ora, spark, statement,  conn_options, conn_user, conn_key)
if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")

if debug:
    display(df_C)

temp_view = f"temp_df_c_{sr_id_archivo}"
df_C.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog_name}.{schema_name}.{temp_view}"
)
del df_C

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parte cuatro

# COMMAND ----------

statement = f"""
SELECT
    O.FCN_ID_SIEFORE,
    O.FCN_ID_GRUPO,
    O.FTN_NUM_CTA_INVDUAL
FROM CIERREN.TTAFOGRAL_OSS O
    JOIN (SELECT DISTINCT S.FCN_ID_GRUPO FROM CIERREN.TCCRXGRAL_SIEFORE S WHERE S.FCC_VIGENCIA = 1) S
        ON O.FCN_ID_GRUPO = S.FCN_ID_GRUPO and O.FTC_ESTATUS = 1 AND O.FTN_PRIORIDAD = 1
"""

# COMMAND ----------

df_D1, failed_task = query_table(conn_name_ora, spark, statement,  conn_options, conn_user, conn_key)
if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")

if debug:
    display(df_D1)

temp_view = f"temp_df_d1_{sr_id_archivo}"
df_D1.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog_name}.{schema_name}.{temp_view}"
)
del df_D1

# COMMAND ----------

statement = f"""
select 
    a.FTN_NUM_CTA_INVDUAL
    from
    (
    select  
    case when 3={p_ind} then FTN_NSS else (case when 2={p_ind} then FTC_CURP else '0'end) end FTN_NSS_CURP
    ,FTN_NUM_CTA_INVDUAL
    from CIERREN.TTAFOGRAL_CTA_INVDUAL) a
    INNER JOIN (SELECT distinct FTC_NSS_CURP FROM  CIERREN_ETL.TTSISGRAL_ETL_DISPERSION_CTAS WHERE FTC_FOLIO= '{sr_folio}') b
    on a.FTN_NSS_CURP = b.FTC_NSS_CURP
"""
print(statement)

# COMMAND ----------

df_D2, failed_task = query_table(conn_name_ora, spark, statement,  conn_options, conn_user, conn_key)
if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")

if debug:
    display(df_D2)

temp_view = f"temp_df_d2_{sr_id_archivo}"
df_D2.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog_name}.{schema_name}.{temp_view}"
)
del df_D2

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join entre df_D1 y df_D2

# COMMAND ----------

statement = f"""
SELECT 
    O.FCN_ID_SIEFORE,
    O.FCN_ID_GRUPO,
    O.FTN_NUM_CTA_INVDUAL
FROM {catalog_name}.{schema_name}.temp_df_d1_{sr_id_archivo} O
INNER JOIN {catalog_name}.{schema_name}.temp_df_d2_{sr_id_archivo} a
ON o.FTN_NUM_CTA_INVDUAL = a.FTN_NUM_CTA_INVDUAL
"""

# COMMAND ----------

df_D = spark.sql(statement)

if debug:
    display(df_D)

temp_view = f"temp_df_d_{sr_id_archivo}"
df_D.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog_name}.{schema_name}.{temp_view}"
)
del df_D

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Parte final 

# COMMAND ----------

statement = f"""
SELECT 
A.FTN_NUM_CTA_INVDUAL
,B.FCN_ID_TIPO_SUBCTA
,MAX(C.FCN_ID_GRUPO) FCN_ID_GRUPO
,MAX(D.FCN_ID_SIEFORE) FCN_ID_SIEFORE
FROM {catalog_name}.{schema_name}.temp_df_a_{sr_id_archivo} A
INNER JOIN {catalog_name}.{schema_name}.temp_df_b_{sr_id_archivo} B
ON A.FTN_NSS_CURP = B.FTN_NSS_CURP
INNER JOIN {catalog_name}.{schema_name}.temp_df_c_{sr_id_archivo} C
ON B.FCN_ID_TIPO_SUBCTA = C.FCN_ID_TIPO_SUBCTA
LEFT JOIN {catalog_name}.{schema_name}.temp_df_d_{sr_id_archivo} D
ON A.FTN_NUM_CTA_INVDUAL = D.FTN_NUM_CTA_INVDUAL
AND C.FCN_ID_GRUPO = D.FCN_ID_GRUPO
GROUP BY A.FTN_NUM_CTA_INVDUAL, B.FCN_ID_TIPO_SUBCTA
"""

# COMMAND ----------

df = spark.sql(statement)

if debug:
    display(df)

# COMMAND ----------

# DBTITLE 1,Genera vista TEMP_Siefore01
from pyspark.sql.functions import when, col

#Agrega columna MARCA con la siguiente condición
#If  IsNull(DSLink221.FCN_ID_SIEFORE)  Then 1 Else  0

# Aplicar la lógica para asignar el valor de la columna 'MARCA'
df = df.withColumn("MARCA", when(col("FCN_ID_SIEFORE").isNull(), 1).otherwise(0))

temp_view = 'TEMP_Siefore01' + '_' + sr_id_archivo
df.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog_name}.{schema_name}.{temp_view}"
)

# spark.catalog.dropGlobalTempView(temp_view)
# df.createOrReplaceGlobalTempView(temp_view)
 
if debug:
    display(df)    

# COMMAND ----------

# Liberar la caché del DataFrame
df.unpersist()

# Si ya no necesitas df en lo absoluto, puedes eliminarlo
del df

# Forzar la recolección de basura para liberar memoria
import gc
gc.collect()
