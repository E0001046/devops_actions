# Databricks notebook source
# DBTITLE 1,CONFIGURACIONES PRINCIPALES
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
# MAGIC def input_values():
# MAGIC     """ Recuperar valores de entrada desde widgets.
# MAGIC     Returns:
# MAGIC         tuple: Una tupla que contiene los valores de entrada y una bandera de estado.
# MAGIC     """
# MAGIC     try:
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
# MAGIC        
# MAGIC         # Obtener valores de los widgets
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
# MAGIC
# MAGIC         # Verificar si algún valor de entrada está vacío o nulo
# MAGIC         if any(len(str(value).strip()) == 0 for value in [sr_folio,sr_proceso,sr_subproceso,sr_subetapa,sr_sec_lote,sr_fecha_lote,sr_tipo_archivo]):    
# MAGIC             logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC             logger.error("Some of the input values are empty or null")
# MAGIC             return '0','0','0','0','0','0','0'
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: %s", str(e))
# MAGIC         return '0','0','0','0','0','0','0','0','0','0','0','0','0','0'
# MAGIC     return sr_folio,sr_proceso,sr_subproceso,sr_origen_arc,sr_dt_org_arc,sr_subetapa,sr_sec_lote,sr_fecha_lote,sr_fecha_acc,sr_tipo_archivo,sr_estatus_mov,sr_tipo_mov,sr_accion,'1'
# MAGIC
# MAGIC def conf_process_values(config_file, process_name):
# MAGIC     """ Recuperar valores de configuración del proceso desde un archivo de configuración.
# MAGIC     Args:
# MAGIC         config_file (str): Ruta al archivo de configuración.
# MAGIC         process_name (str): Nombre del proceso.
# MAGIC     Returns:
# MAGIC         tuple: Una tupla que contiene los valores de configuración y una bandera de estado.
# MAGIC     """
# MAGIC     try:
# MAGIC         config = configparser.ConfigParser()
# MAGIC         config.read(config_file)
# MAGIC         # Configuraciones del subproceso
# MAGIC         sql_conf_file = config.get(process_name, 'sql_conf_file')  # Obtener archivo de configuración SQL
# MAGIC         conn_schema_001 = config.get(process_name, 'conn_schema_001')
# MAGIC         conn_schema_002 = config.get(process_name,'conn_schema_002')
# MAGIC         table_001 = config.get(process_name, 'table_001')
# MAGIC         table_003 = config.get(process_name, 'table_003')
# MAGIC         table_004 = config.get(process_name, 'table_004')
# MAGIC         table_005 = config.get(process_name, 'table_005')
# MAGIC         table_006 = config.get(process_name, 'table_006')
# MAGIC         table_007 = config.get(process_name, 'table_007')
# MAGIC         table_008 = config.get(process_name, 'table_008')
# MAGIC         table_011 = config.get(process_name, 'table_011')
# MAGIC         var_001 = config.get(process_name, 'var_001')
# MAGIC         external_location = config.get(process_name, 'external_location')
# MAGIC         err_repo_path = config.get(process_name, 'err_repo_path')
# MAGIC         sep = config.get(process_name, 'sep')
# MAGIC         header = config.get(process_name, 'header')
# MAGIC         debug = config.get(process_name, "debug")  # Obtener valor de debug
# MAGIC         debug = debug.lower() == "true"  # Convertir valor de debug a booleano
# MAGIC         catalog_name = config.get(process_name, "catalog_name")
# MAGIC         schema_name = config.get(process_name, "schema_name")
# MAGIC     
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC         return '0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0'
# MAGIC     return sql_conf_file, conn_schema_001, conn_schema_002,table_001, table_003,table_004,table_005,table_006,table_007,table_008,table_011,var_001,external_location,err_repo_path,sep,header,debug,catalog_name,schema_name,'1'
# MAGIC
# MAGIC if __name__ == "__main__" :
# MAGIC     logging.basicConfig()  # Configurar logging básico
# MAGIC     logger = logging.getLogger(__name__)  # Obtener logger
# MAGIC     logger.setLevel(logging.DEBUG)  # Establecer nivel de logging a DEBUG
# MAGIC     notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()  # Obtener nombre del notebook
# MAGIC     message = 'NB Error: ' + notebook_name  # Mensaje de error
# MAGIC     source = 'ETL'  # Fuente del mensaje
# MAGIC     root_repo = '/Workspace/Shared/MITAFO'  # Ruta del repositorio raíz
# MAGIC     
# MAGIC     try:
# MAGIC         sys.path.append(root_repo + '/' + 'CGRLS_0010/Notebooks')  # Agregar ruta del notebook al sys.path
# MAGIC         from NB_GRLS_DML_FUNCTIONS import *  # Importar funciones DML
# MAGIC         from NB_GRLS_SIMPLE_FUNCTIONS import *  # Importar funciones simples
# MAGIC     except Exception as e:
# MAGIC         logger.error("Error at the beggining of the process")
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC
# MAGIC     config_file = root_repo + '/' + 'CGRLS_0010/Conf/CF_GRLS_PROCESS.py.properties'  # Ruta del archivo de configuración del proceso
# MAGIC     config_conn_file = root_repo + '/' + 'CGRLS_0010/Conf/CF_GRLS_CONN.py.properties'  # Ruta del archivo de configuración de conexión
# MAGIC     config_process_file = root_repo + '/' + 'ANCIN_0030/Jobs/05_CONCLUSION/01_GENERA_ARCHIVOS/DISPERSIONES/Conf/CF_PART_PROC.py.properties'  # Ruta del archivo de configuración del proceso específico
# MAGIC
# MAGIC     sr_folio,sr_proceso,sr_subproceso,sr_origen_arc,sr_dt_org_arc,sr_subetapa,sr_sec_lote,sr_fecha_lote,sr_fecha_acc,sr_tipo_archivo,sr_estatus_mov,sr_tipo_mov,sr_accion,failed_task= input_values()  # Obtener valores de entrada
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         #notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("An error ocurred, check out log messages")
# MAGIC     
# MAGIC     input_parameters = dbutils.widgets.getAll().items()  # Obtener todos los parámetros de entrada
# MAGIC     
# MAGIC     process_name = 'root'
# MAGIC     webhook_url, channel, failed_task = conf_init_values(config_file, process_name,'CTINDI')  # Obtener valores de configuración inicial
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     process_name = 'root'
# MAGIC     sql_conf_file, conn_schema_001, conn_schema_002, table_001, table_003, table_004, table_005, table_006, table_007, table_008,table_011, var_001,external_location,err_repo_path,sep,header,debug, catalog_name,schema_name, failed_task = conf_process_values(config_process_file, process_name)
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     conn_name_ora = 'jdbc_oracle'
# MAGIC     conn_options, conn_aditional_options, conn_user, conn_key, conn_url,scope, failed_task = conf_conn_values(config_conn_file, conn_name_ora)  # Obtener valores de configuración de conexión
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC     
# MAGIC     sql_conf_file = root_repo + '/' + 'ANCIN_0030/Jobs/05_CONCLUSION/01_GENERA_ARCHIVOS/DISPERSIONES/JSON/' + sql_conf_file  # Actualizar ruta del archivo de configuración SQL

# COMMAND ----------

# DBTITLE 1,EXTRACCIÓN DEL ARCHIVO JSON
#Read Json file & extract the values
with open(sql_conf_file) as f:
    file_config_sql = json.load(f)

conf_values = [ (fields['step_id'], '\n'.join(fields['value'])) for line, value in file_config_sql.items() if line == 'steps' for fields in value ]

# COMMAND ----------

# DBTITLE 1,CONSTRUCCIÓN DE CONSULTA
# Nombre de la tabla compuesto por el esquema y el nombre de la tabla obtenidos de la configuración
table_name_011 = conn_schema_001 + '.' + table_011

# Identificador del statement a ejecutar
query_statement = '010'

# Parámetros para el statement, incluyendo el folio y el nombre de la tabla
params = [sr_fecha_acc, table_name_011, sr_folio]

# Obtención del statement SQL a ejecutar y verificación de errores
statement, failed_task = getting_statement(conf_values, query_statement, params)

# Si la obtención del statement falla, se registra el error y se notifica
if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")  # Finaliza el proceso debido al error

# Si el modo debug está activo, se imprime el statement para revisión
if debug:
    print(statement)

# COMMAND ----------

# DBTITLE 1,EXTRACCIÓN DE INFORMACIÓN
df, failed_task = query_table(conn_name_ora, spark, statement,  conn_options, conn_user, conn_key)

if debug:
    display(df)

# COMMAND ----------

# DBTITLE 1,PRUEBA CON DATOS DE QA - SE DEBE ELIMINAR
# QA
# Configurar los parámetros de conexión
# jdbc_url = """jdbc:oracle:thin:@(DESCRIPTION=(LOAD_BALANCE=yes)
# (ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=10.16.91.14)(PORT=1521))
# (ADDRESS=(PROTOCOL=TCP)(HOST=10.16.91.15)(PORT=1521))
# (ADDRESS=(PROTOCOL=TCP)(HOST=10.16.91.13)(PORT=1521)))
# (CONNECT_DATA=(SERVICE_NAME=mitafoqa.snashqdbsy02.vcnashgrnvcnqa.oraclevcn.com)(SERVER=DEDICATED))
# (FAILOVER_MODE=(TYPE=session)(METHOD=basic)(RETRIES=10)(DELAY=5)))"""

# Propiedades de conexión
# jdbc_properties = {
#     "user": "CIERREN_APP",
#     "password": "l90E5$TT",
#     "driver": "oracle.jdbc.driver.OracleDriver",
#     "fetchSize": "20000",
#     "batchsize": "40000",
#     "numPartitions": "500",
#     "queryTimeout": "0",
#     "connectRetryCount": "10",
#     "connectRetryInterval": "10"
# }

# Ejecutar el query con Spark
# df = spark.read \
#     .format("jdbc") \
#     .option("url", jdbc_url) \
#     .option("query", statement) \
#     .option("user", jdbc_properties["user"]) \
#     .option("password", jdbc_properties["password"]) \
#     .option("driver", jdbc_properties["driver"]) \
#     .option("fetchSize", jdbc_properties["fetchSize"]) \
#     .option("numPartitions", jdbc_properties["numPartitions"]) \
#     .option("batchsize", jdbc_properties["batchsize"]) \
#     .option("queryTimeout", jdbc_properties["queryTimeout"]) \
#     .option("connectRetryCount", jdbc_properties["connectRetryCount"]) \
#     .option("connectRetryInterval", jdbc_properties["connectRetryInterval"]) \
#     .load()

# display(df)

# COMMAND ----------

# DBTITLE 1,VALIDACIÓN DE ARCHIVO VACÍO
if df.count() == 0:
    notification_raised(webhook_url, 0, "DONE", source, input_parameters)
    dbutils.notebook.exit("No hay información para generar el archivo.")

# COMMAND ----------

# DBTITLE 1,CONCATENACIÓN ANTES DE CREAR EL ARCHIVO
from pyspark.sql.functions import concat_ws, lit

# Concatena las columnas especificadas en el DataFrame df
df_sub = df.withColumn(
    "ARCHIVO_SUB",
    concat_ws(
        "",
        lit(""),  # Se inicia con una cadena vacía como primer valor para concat_ws.
        df["SUBSEC_NUMCUE"],
        df["SUBSEC_NSSTRA"],
        df["SUBSEC_VIVIENDA97"],
        df["SUBSEC_FECCON"],
        df["SUBSEC_PERPAG"],
        df["SUBSEC_FECTRA"],
        df["SUBSEC_SECLOT"],
        df["SUBSEC_ESTATUS"],
        df["SUBSEC_PARTICIPA97"],
    ),
)

# Mostrar el DataFrame resultante
df_final = df_sub.select("ARCHIVO_SUB")

if debug:
    display(df_final)

# COMMAND ----------

# DBTITLE 1,CREACIÓN DEL ARCHIVO
from datetime import datetime
import pytz

# Define the timezone for Mexico
mexico_tz = pytz.timezone('America/Mexico_City')

# Genera la fecha actual en el formato YYYYMMDD
fecha_actual = datetime.now(mexico_tz).strftime("%Y%m%d")

# Crea el nombre completo del archivo concatenando la ubicación externa, el camino del repositorio de errores y la fecha actual
full_file_name = external_location + err_repo_path + "/PPA_RCDI00506_" + fecha_actual + "_" + sr_sec_lote + ".DAT"

try:
    # Save in temporary Data Frame
    df_final.write.format("csv").mode("overwrite").option("header", header).save(
        full_file_name + "_TEMP"
    )
    dataframe = spark.read.option("header", header).csv(full_file_name + "_TEMP")
    dataframe.coalesce(1).write.format("csv").mode("overwrite").option("header", header).option("lineSep","\r\n").save(full_file_name)
except Exception as e:
    logger.error("An error was raised: " + str(e))
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

if debug:
    print(full_file_name)

# COMMAND ----------

try:
    dfFile = spark.read.option('header', header).csv(full_file_name)
    dfFile.coalesce(1).write.format('csv').mode('overwrite').option('header', header).option("lineSep","\r\n").save(full_file_name)
except Exception as e:
    logger.error("An error was raised: " + str(e))
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

# COMMAND ----------

# DBTITLE 1,CREACIÓN DE ARCHIVO TEMPORAL
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

# DBTITLE 1,COPIA EL ARCHIVO DE ADLS AL SISTEMA DE ARCHIVOS DATABRICKS
#Copia el archivo generado al volumen asignado para poder descargarlo en el equipo local  PPA_RCDI00506_" + fecha_actual + "_" + sr_sec_lote + .DAT"
# dbutils.fs.cp('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/INTEGRITY/PPA_RCDI00506_' + fecha_actual + "_" + sr_sec_lote + '.DAT',
# '/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/PPA_RCDI00506_' + fecha_actual + "_" + sr_sec_lote + '.DAT')

# COMMAND ----------

# DBTITLE 1,NOTIFICACIÓN
notification_raised(webhook_url, 0, "DONE", source, input_parameters)
