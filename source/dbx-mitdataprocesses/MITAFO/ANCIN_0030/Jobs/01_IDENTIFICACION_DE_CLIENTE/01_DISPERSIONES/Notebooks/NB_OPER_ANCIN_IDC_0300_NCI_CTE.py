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

# Agrego df al cache
df.cache()

if debug:
    display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Indicador 11 es de cliente pensionado.
# MAGIC ### Indicador 5 es la fecha de apertura de la cuenta individual.
# MAGIC ### Indicador 2 es si la cuenta esta vigente.

# COMMAND ----------

# DBTITLE 1,Genera vista intermedia TEMP_IND_CTA_VIG
from pyspark.sql.functions import col, to_date

df1 = df.filter(col("FFN_ID_CONFIG_INDI") == 2).orderBy(
    "FTN_NUM_CTA_INVDUAL",
    ascending=True,
)
df1 = df1.withColumn("FCC_VALOR_IND", col("FCC_VALOR_IND").cast("int"))

temp_view = "TEMP_IND_CTA_VIG_02" + "_" + sr_id_archivo
df1.write.format("delta").mode("overwrite").saveAsTable(
     f"{catalog_name}.{schema_name}.{temp_view}"
)

# spark.catalog.dropGlobalTempView(temp_view)
# df1.createOrReplaceGlobalTempView(temp_view)

if debug:
    display(df1)

del df1

# COMMAND ----------

# DBTITLE 1,Genera vista TEMP_FechaApCtaInd_05
from pyspark.sql.functions import col, to_date

df2 = df.select(
    col("FTN_NUM_CTA_INVDUAL"),
    to_date(col("FCC_VALOR_IND"), "dd/MM/yyyy").alias("FCC_VALOR_IND"),
).filter("FFN_ID_CONFIG_INDI = 5")

temp_view = "TEMP_FechaApCtaInd_05" + "_" + sr_id_archivo
df2.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog_name}.{schema_name}.{temp_view}"
)

# spark.catalog.dropGlobalTempView(temp_view)
# df2.createOrReplaceGlobalTempView(temp_view)

if debug:
    display(df2)

del df2

# COMMAND ----------

# DBTITLE 1,Genera vista TEMP_Pensionado_08
df3 = df.select("FTN_NUM_CTA_INVDUAL","FCC_VALOR_IND").filter("FFN_ID_CONFIG_INDI = 11")

temp_view = 'TEMP_Pensionado_11' + '_' + sr_id_archivo
df3.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog_name}.{schema_name}.{temp_view}"
)

# spark.catalog.dropGlobalTempView(temp_view)
# df3.createOrReplaceGlobalTempView(temp_view)
#spark.sql("select * from global_temp.TEMP_Pensionado_08_29288").show(10)
  
if debug:
    display(df3)

df.unpersist()
del df3, df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Empezamos la version la nueva

# COMMAND ----------

statement = f"""
SELECT FTN_NSS_CURP
 ,FTC_BANDERA
 ,FTN_NUM_CTA_INVDUAL
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
 ,FCN_ID_TIPO_SUBCTA
FROM {catalog_name}.{schema_name}.TEMP_TRAMITE_{sr_id_archivo}
"""
DF_TEMP_TRAMITE = spark.sql(statement)

if debug:
    display(DF_TEMP_TRAMITE)

# COMMAND ----------

DF_TEMP_TRAMITE_ORI = DF_TEMP_TRAMITE
DF_TEMP_TRAMITE = DF_TEMP_TRAMITE.filter(DF_TEMP_TRAMITE.FTC_BANDERA.isNotNull()).orderBy("FTN_NUM_CTA_INVDUAL", ascending=True)
if debug:
    display(DF_TEMP_TRAMITE)

# Creamos una temp view a partir de DF_TEMP_TRAMITE
temp_view = f"temp_view_tramite_no_null_{sr_id_archivo}"
DF_TEMP_TRAMITE.createOrReplaceTempView(temp_view)

# COMMAND ----------

# Hacemos un query entre la vista
statement = f"""
SELECT 
    tra.FTN_NSS_CURP,
    tra.FTN_NUM_CTA_INVDUAL,
    ind.FCN_ID_IND_CTA_INDV,
    ind.FFN_ID_CONFIG_INDI,
    ind.FCC_VALOR_IND,
    ind.FTC_VIGENCIA,
    ind.FTN_DETALLE,
    tra.FTC_NOMBRE_BUC,
    tra.FTC_AP_PATERNO_BUC,
    tra.FTC_AP_MATERNO_BUC,
    tra.FTC_RFC_BUC,
    tra.FTC_FOLIO,
    tra.FTN_ID_ARCHIVO,
    tra.FTN_NSS,
    tra.FTC_CURP,
    tra.FTC_RFC,
    tra.FTC_NOMBRE_CTE,
    tra.FTC_CLAVE_ENT_RECEP,
    tra.FCN_ID_TIPO_SUBCTA
FROM temp_view_tramite_no_null_{sr_id_archivo} tra
LEFT JOIN {catalog_name}.{schema_name}.TEMP_IND_CTA_VIG_02_{sr_id_archivo} ind
ON tra.FTN_NUM_CTA_INVDUAL = ind.FTN_NUM_CTA_INVDUAL
"""
df = spark.sql(statement)
if debug:
    display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC - Aplicamos el filtro donde:
# MAGIC   - FTN_NUM_CTA_INVDUAL isnotnull And FCC_VALOR_IND = 1
# MAGIC   - El resultado del filtro anterior lo guardamos en una tabla delta llamada temp_vigentes_{sr_id_archivo}
# MAGIC
# MAGIC - Aplicamos el filtro donde:
# MAGIC   - FTN_NUM_CTA_INVDUAL isnotnull And FCC_VALOR_IND = 0
# MAGIC   - El resultado del filtro anterior lo guardamos en una tabla delta llamada temp_novigentes_{sr_id_archivo}
# MAGIC
# MAGIC - Aplicamos el filtro que sea todo lo contrario a los anteriores filtros:
# MAGIC   - El resultado del filtro anterior lo guardamos en un df llamado df.

# COMMAND ----------

# Aplicamos el filtro donde FTN_NUM_CTA_INVDUAL isnotnull And FCC_VALOR_IND = 1
df_vigentes = df.filter("FTN_NUM_CTA_INVDUAL IS NOT NULL AND FCC_VALOR_IND = 1")
temp_view_vigentes = f"temp_vigentes_{sr_id_archivo}"
df_vigentes.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.{temp_view_vigentes}")
if debug:
    print("#"*50)
    display(df_vigentes)
del df_vigentes

# Aplicamos el filtro donde FTN_NUM_CTA_INVDUAL isnotnull And FCC_VALOR_IND = 0
df_novigentes = df.filter("FTN_NUM_CTA_INVDUAL IS NOT NULL AND FCC_VALOR_IND = 0")
temp_view_novigentes = f"temp_novigentes_{sr_id_archivo}"
df_novigentes.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.{temp_view_novigentes}")
if debug:
    print("#"*50)
    display(df_novigentes)
del df_novigentes

# Aplicamos el filtro que sea todo lo contrario a los anteriores filtros
df = df.filter("FTN_NUM_CTA_INVDUAL IS NULL OR (FCC_VALOR_IND != 1 AND FCC_VALOR_IND != 0)")
if debug:
    print("#"*50)
    display(df)

# COMMAND ----------

from pyspark.sql.functions import lit

df = df.select(
    "FTN_NSS_CURP",
    "FTN_NUM_CTA_INVDUAL",
    "FTC_NOMBRE_BUC",
    "FTC_AP_PATERNO_BUC",
    "FTC_AP_MATERNO_BUC",
    "FTC_RFC_BUC",
    "FTC_FOLIO",
    "FTN_ID_ARCHIVO",
    "FTN_NSS",
    "FTC_CURP",
    "FTC_RFC",
    "FTC_NOMBRE_CTE",
    "FTC_CLAVE_ENT_RECEP",
    "FCN_ID_TIPO_SUBCTA"
).withColumn("FTC_IDENTIFICADOS", lit(0)) \
 .withColumn("FTN_ID_DIAGNOSTICO", lit(375)) \
 .withColumn("FTN_VIGENCIA", lit(0))

if debug:
    display(df)

# COMMAND ----------

from pyspark.sql.functions import lit

DF_TEMP_TRAMITE_ORI = (
    DF_TEMP_TRAMITE_ORI.filter(DF_TEMP_TRAMITE_ORI.FTC_BANDERA.isNull())
    .withColumn("FTC_IDENTIFICADOS", lit(0))
    .withColumn("FTN_ID_DIAGNOSTICO", lit(93))
    .withColumn("FTN_VIGENCIA", lit(0))
)

columnas = [
    "FTN_NSS_CURP",
    "FTN_NUM_CTA_INVDUAL",
    "FTC_NOMBRE_BUC",
    "FTC_AP_PATERNO_BUC",
    "FTC_AP_MATERNO_BUC",
    "FTC_RFC_BUC",
    "FTC_FOLIO",
    "FTN_ID_ARCHIVO",
    "FTN_NSS",
    "FTC_CURP",
    "FTC_RFC",
    "FTC_NOMBRE_CTE",
    "FTC_CLAVE_ENT_RECEP",
    "FCN_ID_TIPO_SUBCTA",
    "FTC_IDENTIFICADOS",
    "FTN_ID_DIAGNOSTICO",
    "FTN_VIGENCIA",
]
DF_TEMP_TRAMITE_ORI = DF_TEMP_TRAMITE_ORI.select(*columnas)
if debug:
    display(DF_TEMP_TRAMITE_ORI)

# COMMAND ----------

df_final = df.union(DF_TEMP_TRAMITE_ORI)
temp_view_noencontrados = f"temp_noencontrados_{sr_id_archivo}"
df_final.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.{temp_view_noencontrados}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Hacia abajo es la version original

# COMMAND ----------

# query_statement = '016'

# params = [sr_id_archivo]

# statement, failed_task = getting_statement(conf_values, query_statement, params)

# if failed_task == '0':
#     logger.error("No value %s found", statement)
#     logger.error("Please review log messages")
#     notification_raised(webhook_url, -1, message, source, input_parameters)
#     raise Exception("Process ends")

# print(statement)

# df_tramite = spark.sql(statement)

# df_tramite.cache() #Se usa para otras cosas al final

# if failed_task == '0':
#     logger.error("Please review log messages")
#     notification_raised(webhook_url, -1, message, source, input_parameters)
#     raise Exception("An error raised")


# COMMAND ----------

# DBTITLE 1,Genera Vista intermedia Temp_tramite
# temp_view = 'Temp_tramite' + '_' + sr_id_archivo
# df_tramite.write.format("delta").mode("overwrite").saveAsTable(
#     f"{catalog_name}.{schema_name}.{temp_view}"
# )

# #df_tramite.createOrReplaceTempView(temp_view)

# if debug:
#     display(df_tramite)

# COMMAND ----------

# query_statement = '017'
# catalog_schema = f"{catalog_name}.{schema_name}"

# params = [sr_id_archivo,catalog_schema]

# statement, failed_task = getting_statement(conf_values, query_statement, params)

# if failed_task == '0':
#     logger.error("No value %s found", statement)
#     logger.error("Please review log messages")
#     notification_raised(webhook_url, -1, message, source, input_parameters)
#     raise Exception("Process ends")

# df = spark.sql(statement)

# if failed_task == '0':
#     logger.error("Please review log messages")
#     notification_raised(webhook_url, -1, message, source, input_parameters)
#     raise Exception("An error raised")

# df.cache()

# print(statement)
# if debug:
#     display(df)

# COMMAND ----------

# DBTITLE 1,Genera TEMP_Vigentes_02
# df1 = df.filter("FTN_NUM_CTA_INVDUAL IS NOT NULL AND FCC_VALOR_IND = 1")

# temp_view = 'TEMP_Vigentes_02' + '_' + sr_id_archivo
# df1.write.format("delta").mode("overwrite").saveAsTable(
#     f"{catalog_name}.{schema_name}.{temp_view}"
# )

# #spark.catalog.dropGlobalTempView(temp_view)
# #df1.createOrReplaceGlobalTempView(temp_view)
    
# if debug:
#     display(df1)

# del df1

# COMMAND ----------

# DBTITLE 1,Genera TEMP_NoVigentes_03
# df2 = df.filter("FTN_NUM_CTA_INVDUAL IS NOT NULL AND FCC_VALOR_IND = 0")

# temp_view = 'TEMP_NoVigentes_03' + '_' + sr_id_archivo
# df2.write.format("delta").mode("overwrite").saveAsTable(
#     f"{catalog_name}.{schema_name}.{temp_view}"
# )

# # spark.catalog.dropGlobalTempView(temp_view)
# # df2.createOrReplaceGlobalTempView(temp_view)
    
# if debug:
#     display(df2)

# del df2

# COMMAND ----------

# DBTITLE 1,Genera TEMP_NoEncontrados_04
# #375	INDICADOR NO ENCONTRADO
# #Crea df3, filtra y agrega columnas 
# df3 = df.filter("((FCC_VALOR_IND <> 1 and FCC_VALOR_IND <> 0) OR FCC_VALOR_IND IS NULL)")

# # Liberar la caché del DataFrame
# df.unpersist()

# df3.cache()

# # Si ya no necesitas df en lo absoluto, puedes eliminarlo
# del df

# df3 =  df3.select("FTN_NSS_CURP" ,"FTN_NUM_CTA_INVDUAL" ,"FTC_NOMBRE_BUC" ,"FTC_AP_PATERNO_BUC" ,"FTC_AP_MATERNO_BUC" ,"FTC_RFC_BUC" ,"FTC_FOLIO" ,"FTN_ID_ARCHIVO" ,"FTN_NSS" ,"FTC_CURP" ,"FTC_RFC" ,"FTC_NOMBRE_CTE" ,"FTC_CLAVE_ENT_RECEP", "FCN_ID_TIPO_SUBCTA", lit(0).alias("FTC_IDENTIFICADOS"), lit(375).alias("FTN_ID_DIAGNOSTICO"), lit(0).alias("FTN_VIGENCIA"))

# if debug:
#     display(df3)

# #93	CLIENTE NO ENCONTRADO
# #Agrega columnas FTC_IDENTIFICADOS, FTN_ID_DIAGNOSTICO, FTN_VIGENCIA
# df_tramite = df_tramite.filter("FTC_BANDERA IS NULL")
# df_tramite = df_tramite.select ("FTN_NSS_CURP" ,"FTN_NUM_CTA_INVDUAL" ,"FTC_NOMBRE_BUC" ,"FTC_AP_PATERNO_BUC" ,"FTC_AP_MATERNO_BUC" ,"FTC_RFC_BUC" ,"FTC_FOLIO" ,"FTN_ID_ARCHIVO" ,"FTN_NSS" ,"FTC_CURP" ,"FTC_RFC" ,"FTC_NOMBRE_CTE" ,"FTC_CLAVE_ENT_RECEP","FCN_ID_TIPO_SUBCTA", lit(0).alias("FTC_IDENTIFICADOS"), lit(93).alias("FTN_ID_DIAGNOSTICO"), lit(0).alias("FTN_VIGENCIA"))


# #Union df_tramite y df3
# df_NoEncontrados = df_tramite.union(df3)

# temp_view = 'TEMP_NoEncontrados_04' + '_' + sr_id_archivo
# df_NoEncontrados.write.format("delta").mode("overwrite").saveAsTable(
#     f"{catalog_name}.{schema_name}.{temp_view}"
# )

# # spark.catalog.dropGlobalTempView(temp_view)
# # df_NoEncontrados.createOrReplaceGlobalTempView(temp_view)

# if debug:
#     display(df_NoEncontrados)

# COMMAND ----------

# Liberar la caché del DataFrame si se usó cache

# Eliminar DataFrames para liberar memoria
del DF_TEMP_TRAMITE_ORI, df_final

# Recolector de basura para liberar recursos inmediatamente
import gc
gc.collect()
