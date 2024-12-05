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
# MAGIC     """ Retrieve input values from widgets.
# MAGIC     Returns:
# MAGIC         tuple: A tuple containing the input values and a status flag.
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
# MAGIC
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
# MAGIC         sql_conf_file = config.get(process_name, 'sql_conf_file')
# MAGIC         conn_schema_001 = config.get(process_name,'conn_schema_001')
# MAGIC         table_011 = config.get(process_name, 'table_011')
# MAGIC         external_location = config.get(process_name, 'external_location')
# MAGIC         err_repo_path = config.get(process_name, 'err_repo_path')
# MAGIC         sep = config.get(process_name, 'sep')
# MAGIC         header = config.get(process_name, 'header')
# MAGIC         debug = config.get(process_name, "debug")
# MAGIC         debug = debug.lower() == "true"
# MAGIC
# MAGIC     
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC         return '0','0','0','0','0','0','0','0','0','0'
# MAGIC     return sql_conf_file, conn_schema_001,table_011,external_location,err_repo_path,sep,header,debug, '1'
# MAGIC
# MAGIC
# MAGIC '''def fix_created_file(file_name):
# MAGIC     try:
# MAGIC         file_name_tmp=dbutils.fs.ls(file_name + '_TEMP')
# MAGIC         file_name_new=list(filter(lambda x : x[0].endswith('DAT'), file_name_tmp))
# MAGIC         dbutils.fs.mv(file_name_new[0][0], file_name)
# MAGIC         dbutils.fs.rm(file_name + '_TEMP', recurse = True)
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: " + str(inspect.stack()[0][3]))
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC         return '0'
# MAGIC     return '1'''
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
# MAGIC     config_process_file = root_repo + '/' + 'ANCIN_0030/Jobs/05_CONCLUSION/01_GENERA_ARCHIVOS/DISPERSIONES/Conf/CF_PART_PROC.py.properties'
# MAGIC
# MAGIC     sr_folio,sr_proceso,sr_subproceso,sr_origen_arc,sr_dt_org_arc,sr_subetapa,sr_sec_lote,sr_fecha_lote,sr_fecha_acc,sr_tipo_archivo,sr_estatus_mov,sr_tipo_mov,sr_accion,failed_task= input_values()
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         #notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("An error ocurred, check out log messages")
# MAGIC     
# MAGIC     input_parameters = dbutils.widgets.getAll().items()
# MAGIC     
# MAGIC     process_name = 'root'
# MAGIC     webhook_url, channel, failed_task = conf_init_values(config_file, process_name,'CTINDI')
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     process_name = 'root'
# MAGIC     sql_conf_file, conn_schema_001,table_011,external_location,err_repo_path,sep,header,debug,failed_task = conf_process_values(config_process_file, process_name)
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     conn_name_ora = 'jdbc_oracle'
# MAGIC     conn_options, conn_aditional_options, conn_user, conn_key, conn_url, failed_task = conf_conn_values(config_conn_file, conn_name_ora)
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC     
# MAGIC     sql_conf_file = root_repo + '/' + 'ANCIN_0030/Jobs/05_CONCLUSION/01_GENERA_ARCHIVOS/DISPERSIONES/JSON/' + sql_conf_file
# MAGIC

# COMMAND ----------

# DBTITLE 1,ARCHIVO DE CONFIGURACIÓN SQL Y JSON
with open(sql_conf_file) as f:
    # Abre el archivo de configuración SQL y lo carga como un objeto JSON
    file_config_sql = json.load(f)

# Extrae los valores de configuración en una lista de tuplas
conf_values = [
    (fields['step_id'], '\n'.join(fields['value']))  # Crea una tupla con 'step_id' y los valores concatenados con saltos de línea
    for line, value in file_config_sql.items()  # Itera sobre los elementos del archivo JSON
    if line == 'steps'  # Filtra solo los elementos donde la clave es 'steps'
    for fields in value  # Itera sobre los campos dentro de 'steps'
]

# COMMAND ----------

# DBTITLE 1,CONSTRUCCIÓN DE CONSULTA
# Consulta final para construir el archivo CTINDI
table_name_001 = conn_schema_001 + '.' + table_011  # Construye el nombre completo de la tabla usando el esquema y el nombre de la tabla

query_statement = '007'  # Define el identificador de la consulta que se va a utilizar

params = [table_name_001, sr_folio]  # Define los parámetros que se pasarán a la consulta: el nombre de la tabla y el folio

statement, failed_task = getting_statement(conf_values, query_statement, params)  # Obtiene la declaración SQL usando los valores de configuración, el identificador de la consulta y los parámetros

if failed_task == '0':  # Verifica si la tarea falló
    logger.error("No value %s found", statement)  # Registra un error indicando que no se encontró el valor de la declaración
    logger.error("Please review log messages")  # Registra un mensaje de error adicional
    notification_raised(webhook_url, -1, message, source, input_parameters)  # Envía una notificación de error
    raise Exception("Process ends")  # Lanza una excepción para detener el proceso

if debug:
    print(statement)

# COMMAND ----------

# DBTITLE 1,EXTRACCIÓN DE INFORMACIÓN
# Ejecuta la consulta en la base de datos Oracle y obtiene el DataFrame resultante y el estado de la tarea
df, failed_task = query_table(conn_name_ora, spark, statement, conn_options, conn_user, conn_key)

if debug:
    display(df)

# COMMAND ----------

# DBTITLE 1,CONCATENAR ANTES DE CREAR EL ARCHIVO
from pyspark.sql.functions import concat_ws

# Concatenar las columnas especificadas en una sola columna "ARCHIVO_CTINDI" sin separador
df = df.withColumn(
    "ARCHIVO_CTINDI", 
    concat_ws(
        "", 
        df["CVE_SIEFORE"], df["FTN_NUM_CTA_INVDUAL"], df["DVCUE"], df["NUMREG"], 
        df["FFN_CODMOV"], df["NSSTRA"], df["FCD_FECPRO"], df["SECPRO"], 
        df["FTD_FEHCCON"], df["FCN_VALOR_ACCION"], df["FECTRA"], df["SECLOT"], 
        df["FNN_CORREL"], df["FND_FECHA_PAGO"], df["FNC_PERIODO_PAGO_PATRON"], 
        df["FNC_FOLIO_PAGO_SUA"], df["FECSUA"], df["NSSEMP"], df["MONPES1"], 
        df["MONPES2"], df["MONPES3"], df["MONPES4"], df["MONPES5"], df["MONPES6"], 
        df["MONPES7"], df["MONPES8"], df["MONPES9"], df["MONCUO1"], df["MONCUO2"], 
        df["MONCUO3"], df["MONCUO4"], df["MONCUO5"], df["MONCUO6"], df["MONCUO7"], 
        df["MONCUO8"], df["MONCUO9"], df["FECHA_1"], df["FECHA_2"], df["FND_FECVRCV"], 
        df["FND_FECHA_VALOR_IMSS_ACV_VIV"], df["FECVGUB"], df["CVESERV"], df["VARMAX"]
    )
)

# Seleccionar solo la columna "ARCHIVO_CTINDI" y asignarla a un nuevo DataFrame
df_final = df.select("ARCHIVO_CTINDI")

if debug:
    display(df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC **EL CÓDIGO FUNCIONA PARA CREAR ARCHIVOS CSV SEPARADO POR COMAS**

# COMMAND ----------

# DBTITLE 1,CREACIÓN DE ARCHIVO CTINDI
# Primera versión
# CREACIÓN DEL ARCHIVO CTINDI

from datetime import datetime

# Genera la fecha actual en el formato YYYYMMDD
fecha_actual = datetime.now().strftime("%Y%m%d")

# Crea el nombre completo del archivo concatenando la ubicación externa, el camino del repositorio de errores y la fecha actual
full_file_name = external_location + err_repo_path + "/CTINDI1_" + fecha_actual + "_001.DAT"

try:
    # Guarda temporalmente el DataFrame en formato CSV
    df_final.write.format("csv").mode("overwrite").option("header", header).save(
        full_file_name + "_TEMP"
    )
    # Lee el archivo temporal en un nuevo DataFrame
    dataframe = spark.read.option("header", header).csv(full_file_name + "_TEMP")
    # Guarda el DataFrame en un solo archivo CSV
    dataframe.coalesce(1).write.format("csv").mode("overwrite").option(
        "header", header
    ).save(full_file_name)
except Exception as e:
    # Registra el error en el logger
    logger.error("An error was raised: " + str(e))
    # Envía una notificación de error
    notification_raised(webhook_url, -1, message, source, input_parameters)
    # Lanza una excepción para finalizar el proceso
    raise Exception("Process ends")

# COMMAND ----------

try:
    # Lee el archivo CSV en un DataFrame, con la opción de encabezado según la variable 'header'
    dfFile = spark.read.option('header', header).csv(full_file_name)
    
    # Escribe el DataFrame en un solo archivo CSV, con la opción de encabezado según la variable 'header'
    dfFile.coalesce(1).write.format('csv').mode('overwrite').option('header', header).save(full_file_name)
except Exception as e:
    # Registra el error en el logger
    logger.error("An error was raised: " + str(e))
    
    # Envía una notificación de error
    notification_raised(webhook_url, -1, message, source, input_parameters)
    
    # Lanza una excepción para finalizar el proceso
    raise Exception("Process ends")

# COMMAND ----------

# DBTITLE 1,CREA ARCHIVO TEMPORAL Y ELIMINA
try:
    # Define el nombre del archivo temporal auxiliar
    full_file_name_aux = full_file_name + '_TEMP'
    
    # Elimina el archivo temporal auxiliar si existe
    dbutils.fs.rm(full_file_name_aux, recurse=True)

    # Lista los archivos en el directorio del archivo original
    file_name_tmp = dbutils.fs.ls(full_file_name)
    
    # Filtra la lista de archivos para encontrar el archivo CSV
    file_name_new = list(filter(lambda x: x[0].endswith('csv'), file_name_tmp))

    # Copia el archivo CSV encontrado al archivo temporal auxiliar
    dbutils.fs.cp(file_name_new[0][0], full_file_name_aux)
    
    # Elimina el archivo original
    dbutils.fs.rm(full_file_name, recurse=True)
    
    # Copia el archivo temporal auxiliar al nombre del archivo original
    dbutils.fs.cp(full_file_name_aux, full_file_name)
    
    # Elimina el archivo temporal auxiliar
    dbutils.fs.rm(full_file_name_aux, recurse=True)
except Exception as e:
    # Registra el error en el logger
    logger.error("An error was raised: " + str(e))
    
    # Envía una notificación de error
    notification_raised(webhook_url, -1, message, source, input_parameters)
    
    # Lanza una excepción para finalizar el proceso
    raise Exception("Process ends")

# COMMAND ----------

# DBTITLE 1,COPIA EL ARCHIVO AL VOLUMEN ASIGNADO
# Copia el archivo generado al volumen asignado para poder descargarlo en el equipo local
dbutils.fs.cp(
    'abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/OUT/CTINDI1_' + fecha_actual + '_001.DAT', 
    '/Volumes/dbx_mit_dev_1udbvf_workspace/default/doimss_carga_archivo/CTINDI1_' + fecha_actual + '_001.DAT'
)

# COMMAND ----------

# MAGIC %md
# MAGIC **FIN DE LA CREACIÓN DEL ARCHIVO CSV**

# COMMAND ----------

# MAGIC %md
# MAGIC ### OTRA MANERA DE GENERAR EL ARCHIVO - ERROR

# COMMAND ----------

# DBTITLE 1,ASIGNACIÓN DE DATAFRAME
'''df_output = (df)'''

# COMMAND ----------

# DBTITLE 1,CREACIÓN DE ARCHIVO - ERROR
'''#Error al ejecutar este código - se utiliza la funcion created_file
from datetime import datetime

# Genera la fecha actual
fecha_actual = datetime.now().strftime("%Y%m%d")

# Crea el nombre completo del archivo
full_file_name = external_location + err_repo_path + "/CTINDI1_" + fecha_actual + "_001.DAT"

# Crea el archivo
failed_task = create_file(df, full_file_name, sep, header)

#Manejo de errores
if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")

failed_task = fix_created_file(full_file_name)

if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")'''

# COMMAND ----------

# MAGIC %md
# MAGIC ### CÓDIGO DE APOYO

# COMMAND ----------

'''df.select(df.columns).show()'''


# COMMAND ----------

'''display(df.columns)'''

# COMMAND ----------

# DBTITLE 1,UNA SOLA LINEA CONCATENACIÓN
'''from pyspark.sql.functions import concat_ws

df = df.withColumn("ARCHIVO_CTINDI", concat_ws("", df["CVE_SIEFORE"], df["FTN_NUM_CTA_INVDUAL"]))
df_final = df.select("ARCHIVO_CTINDI")
display(df_final)'''

# COMMAND ----------

# DBTITLE 1,CREACIÓN DEL ARCHIVO
'''# CREACIÓN DEL ARCHIVO CTINDI

from datetime import datetime

# Genera la fecha actual
fecha_actual = datetime.now().strftime("%Y%m%d")

# Crea el nombre completo del archivo
full_file_name = external_location + err_repo_path + "/CTINDI_" + fecha_actual + "_005.DAT"

try:
    # Guardar temporalmente el DataFrame
    df_final.write.format("text").mode("overwrite").option("header", header).save(
        full_file_name + "_TEMP"
    )
    dataframe = spark.read.option("header", header).csv(full_file_name + "_TEMP")
    dataframe.coalesce(1).write.format("text").mode("overwrite").option(
        "header", header
    ).save(full_file_name)
except Exception as e:
    logger.error("An error was raised: " + str(e))
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")'''

# COMMAND ----------

# DBTITLE 1,no funciona
'''# CREACIÓN DEL ARCHIVO CTINDI

from datetime import datetime

# Genera la fecha actual
fecha_actual = datetime.now().strftime("%Y%m%d")

# Crea el nombre completo del archivo
full_file_name = external_location + err_repo_path + "/CTINDI_" + fecha_actual + "_001.DAT"

df_final.write.mode('overwrite').format('text').option('header', 'false').option('delimiter', '').save(full_file_name)'''

# COMMAND ----------

# DBTITLE 1,LISTA LOS ARCHIVOS GENERADOS
'''dbutils.fs.ls('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/OUT')'''

# COMMAND ----------

# DBTITLE 1,MUESTRA PARTE DEL ARCHIVO GENERADO
'''dbutils.fs.head('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/OUT/CTINDI_20240824_001.DAT', 100)'''

# COMMAND ----------

# DBTITLE 1,ELIMINA ARCHIVOS
'''dbutils.fs.rm('abfss://nci-repository@datalakedev1udbvf.dfs.core.windows.net/RCDI/OUT/CTINDI_20240826_001.DAT',  recurse=True)'''
