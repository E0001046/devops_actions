# Databricks notebook source
# DBTITLE 1,VARIABLES PRINCIPALES DE CONFIGURACIÓN
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
# MAGIC # Función para obtener valores de entrada desde widgets
# MAGIC def input_values():
# MAGIC     """ Retrieve input values from widgets.
# MAGIC     Returns:
# MAGIC         tuple: A tuple containing the input values and a status flag.
# MAGIC     """
# MAGIC     try:
# MAGIC         # Definición de widgets para la entrada de valores
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
# MAGIC         dbutils.widgets.text('sr_instancia_proceso', '')
# MAGIC         dbutils.widgets.text('sr_usuario', '')
# MAGIC         dbutils.widgets.text('sr_etapa', '')
# MAGIC         dbutils.widgets.text('sr_id_snapshot', '')
# MAGIC
# MAGIC         # Obtención de valores desde los widgets
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
# MAGIC         sr_instancia_proceso = dbutils.widgets.get('sr_instancia_proceso')
# MAGIC         sr_usuario = dbutils.widgets.get('sr_usuario')
# MAGIC         sr_etapa = dbutils.widgets.get('sr_etapa')
# MAGIC         sr_id_snapshot = dbutils.widgets.get('sr_id_snapshot')
# MAGIC
# MAGIC         # Verificación de valores vacíos o nulos
# MAGIC         if any(len(str(value).strip()) == 0 for value in [sr_folio,sr_proceso,sr_subproceso,sr_subetapa,sr_instancia_proceso, sr_usuario, sr_etapa, sr_id_snapshot]):    
# MAGIC             logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC             logger.error("Some of the input values are empty or null")
# MAGIC             return '0','0','0','0'
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: %s", str(e))
# MAGIC         return '0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0'
# MAGIC     return sr_folio,sr_proceso,sr_subproceso,sr_origen_arc,sr_dt_org_arc,sr_subetapa,sr_sec_lote,sr_fecha_lote,sr_fecha_acc,sr_tipo_archivo,sr_estatus_mov,sr_tipo_mov,sr_accion,sr_instancia_proceso, sr_usuario, sr_etapa, sr_id_snapshot,'1'
# MAGIC
# MAGIC # Función para obtener valores de configuración del proceso desde un archivo de configuración
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
# MAGIC         # Configuraciones del subproceso
# MAGIC         # Por favor, revisar el archivo de configuración y verificar todos los esquemas, tablas y vistas
# MAGIC         sql_conf_file = config.get(process_name, 'sql_conf_file')
# MAGIC         conn_schema_001 = config.get(process_name, 'conn_schema_001')
# MAGIC         table_001 = config.get(process_name, 'table_001')
# MAGIC         catalog_001 = config.get (process_name, 'catalog_001')
# MAGIC         conn_schema_002 = config.get(process_name, 'conn_schema_002')
# MAGIC         table_002 = config.get(process_name, 'table_002')
# MAGIC         conn_schema_003 = config.get(process_name, 'conn_schema_003')
# MAGIC         table_003 = config.get(process_name, 'table_003')
# MAGIC         debug = config.get(process_name, "debug")
# MAGIC         debug = debug.lower() == "true"
# MAGIC     
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC         return '0','0','0','0','0','0','0','0'
# MAGIC     return sql_conf_file,catalog_001, conn_schema_001,conn_schema_002,table_001,table_002, conn_schema_003,table_003,debug,'1'
# MAGIC
# MAGIC # Función principal
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
# MAGIC         # Agrega la ruta del repositorio raíz al path del sistema
# MAGIC         sys.path.append(root_repo + '/' + 'CGRLS_0010/Notebooks')
# MAGIC         from NB_GRLS_DML_FUNCTIONS import *
# MAGIC         from NB_GRLS_SIMPLE_FUNCTIONS import *
# MAGIC     except Exception as e:
# MAGIC         logger.error("Error al inicio del proceso")
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC     
# MAGIC     # Obtiene los valores desde las rutas del workspace
# MAGIC     config_file = root_repo + '/' + 'CGRLS_0010/Conf/CF_GRLS_PROCESS.py.properties'
# MAGIC     config_conn_file = root_repo + '/' + 'CGRLS_0010/Conf/CF_GRLS_CONN.py.properties'
# MAGIC     config_process_file = root_repo + '/' + 'ANCIN_0030/Jobs/05_CONCLUSION/05_TRANSF_EXT/Conf/CF_PART_PROC.py.properties'
# MAGIC
# MAGIC     # Obtiene los valores de entrada
# MAGIC     sr_folio,sr_proceso,sr_subproceso,sr_origen_arc,sr_dt_org_arc,sr_subetapa,sr_sec_lote,sr_fecha_lote,sr_fecha_acc,sr_tipo_archivo,sr_estatus_mov,sr_tipo_mov,sr_accion,sr_instancia_proceso, sr_usuario, sr_etapa, sr_id_snapshot,failed_task= input_values()
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         #notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("An error ocurred, check out log messages")
# MAGIC     
# MAGIC     input_parameters = dbutils.widgets.getAll().items()
# MAGIC     
# MAGIC     # Inicializa valores de configuración
# MAGIC     process_name = 'root'
# MAGIC     webhook_url, channel, failed_task = conf_init_values(config_file, process_name, 'VAL_TRANF_EXTEMP')
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     # Obtiene valores de configuración del proceso
# MAGIC     process_name = 'root'
# MAGIC     sql_conf_file,catalog_001, conn_schema_001,conn_schema_002,table_001,table_002,conn_schema_003,table_003,debug,failed_task = conf_process_values(config_process_file, process_name)
# MAGIC
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     # Obtiene valores de configuración de conexión
# MAGIC     conn_name_ora = 'jdbc_oracle'
# MAGIC     conn_options, conn_aditional_options, conn_user, conn_key, conn_url, scope, failed_task = conf_conn_values(config_conn_file, conn_name_ora)
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC     
# MAGIC     # Ruta del archivo JSON
# MAGIC     sql_conf_file = root_repo + '/' + 'ANCIN_0030/Jobs/05_CONCLUSION/05_TRANSF_EXT/JSON/' + sql_conf_file

# COMMAND ----------

# DBTITLE 1,EXTRACCIÓN DEL ARCHIVO JSON
# Lee archivo Json y extraer los valores
with open(sql_conf_file) as f:
    file_config_sql = json.load(f)

# Extrae los valores de configuración del archivo JSON
conf_values = [ (fields['step_id'], '\n'.join(fields['value'])) for line, value in file_config_sql.items() if line == 'steps' for fields in value ]

# COMMAND ----------

# DBTITLE 1,CONSTRUCCIÓN DE CONSULTA VAL IDENT CTE
# Definición de la tabla 001 (CIERREN_ETL.TTSISGRAL_ETL_VAL_IDENT_CTE)
table_name_001 =  conn_schema_001 + '.' + table_001

# Declaración de la consulta 001
query_statement = '001'

# Parámetros para la consulta
params = [sr_folio, table_name_001]

# Obtiene la declaración de la consulta
statement, failed_task = getting_statement(conf_values, query_statement, params)

# Verifica si la tarea falló
if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

# Si está en modo debug, imprime la declaración de la consulta
if debug:
    print(statement)

# COMMAND ----------

# DBTITLE 1,EXTRACCIÓN DE INFORMACIÓN PARA VAL IDENT CTE
#Query 001 execution
# Ejecuta la consulta y obtiene el DataFrame y el estado de la tarea
df_val_ident, failed_task = query_table(conn_name_ora, spark, statement,  conn_options, conn_user, conn_key)

# Si está en modo debug True, imprime la declaración de la consulta
if debug:
    display(df_val_ident)

# COMMAND ----------

# DBTITLE 1,CONSTRUCCIÓN DE CONSULTA ORACLE DISPERSIONES
# Definición de la tabla 002 (ORACLE_DISPERSIONES)
table_name_002 =  catalog_001 + '.' + conn_schema_002 + '.' + table_002 + '_'+ sr_folio

# Declaración de la consulta 002
query_statement = '002'

# Parámetros para la consulta
params = [table_name_002]

# Obtiene la declaración de la consulta
statement, failed_task = getting_statement(conf_values, query_statement, params)

# Verifica si la tarea falló
if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

# Si está en modo debug, imprime la declaración de la consulta
if debug:
    print(statement)

# COMMAND ----------

# DBTITLE 1,EXTRACCIÓN DE ORACLE DISPERSIONES
# Ejecuta la consulta SQL y obtiene el DataFrame resultante
df_mov_disper = spark.sql(statement)

# Si está en modo debug True, muestra el DataFrame resultante
if debug:
    display(df_mov_disper)

# COMMAND ----------

# DBTITLE 1,INNER JOIN VAL CLIENTE Y MOVIMIENTOS
from pyspark.sql.functions import lit, to_timestamp
from datetime import datetime
import pytz

# Define la zona horaria para México
mexico_tz = pytz.timezone('America/Mexico_City')

# Genera la fecha actual
fecha_actual = datetime.now(mexico_tz)

# Realiza la unión de los DataFrames y selecciona las columnas necesarias
df_final = df_mov_disper.join(
    df_val_ident,
    on='FTN_NUM_CTA_INVDUAL',
    how='inner'
).select(
    'FTC_FOLIO',
    'FTN_NUM_CTA_INVDUAL',
    df_val_ident.FTC_NOMBRE_CTE.alias('FTC_NOMBRE'),
    df_val_ident.FTC_RFC.alias('FTC_RFC_CTE'),
    'FTC_CURP',
    'FTD_FEH_LIQUIDACION',
    to_timestamp(lit(fecha_actual),"dd/MM/yy HH:mm:ss").alias('FCD_FEH_ACT'),
    lit('DATABRICKS').alias('FCC_USU_ACT'),
    lit(sr_subproceso).alias('FCN_ID_SUBPROCESO')
)

# Si está en modo debug True, muestra el DataFrame resultante
if debug:
    display(df_final)

# COMMAND ----------

# DBTITLE 1,INSERCIÓN DE DATOS A TABLA EXTEMP
# Inserta valores en la tabla PROCESOS.TTAFOGRAL_TRANSF_EXTEMP
mode = 'APPEND'

# Define el nombre de la tabla con el esquema de conexión
table_name_003 = conn_schema_003 + '.' + table_003

# Escribe en la tabla y captura si la tarea falló
failed_task = write_into_table(conn_name_ora, df_final, mode, table_name_003, conn_options, conn_aditional_options, conn_user, conn_key)

# Si la tarea falló, registra un error y lanza una excepción
if failed_task == '0':
     logger.error("Please review log messages")
     notification_raised(webhook_url, -1, message, source, input_parameters)
     raise Exception("An error raised")

# Notificación de finalización del proceso
notification_raised(webhook_url, 0, "Done", source, input_parameters)
