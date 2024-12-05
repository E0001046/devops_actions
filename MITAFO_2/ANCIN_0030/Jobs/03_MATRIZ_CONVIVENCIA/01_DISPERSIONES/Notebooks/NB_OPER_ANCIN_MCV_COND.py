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
# MAGIC         #Define widgets de texto en Databricks para capturar valores de entrada.
# MAGIC         dbutils.widgets.text('sr_tipo_mov', '')
# MAGIC         dbutils.widgets.text('sr_proceso', '')
# MAGIC         dbutils.widgets.text('sr_subproceso', '')
# MAGIC         dbutils.widgets.text('sr_origen_arc', '')
# MAGIC         dbutils.widgets.text('sr_dt_org_arc', '')
# MAGIC         dbutils.widgets.text('sr_fecha_acc', '')       
# MAGIC         dbutils.widgets.text('sr_subetapa', '')
# MAGIC         dbutils.widgets.text('sr_folio', '')
# MAGIC         dbutils.widgets.text('sr_folio_rel', '')
# MAGIC         dbutils.widgets.text('sr_reproceso', '')
# MAGIC         dbutils.widgets.text('sr_aeim', '')
# MAGIC         dbutils.widgets.text('sr_instancia_proceso', '')
# MAGIC         dbutils.widgets.text('sr_usuario', '')
# MAGIC         dbutils.widgets.text('sr_etapa', '')
# MAGIC         dbutils.widgets.text('sr_id_snapshot', '')
# MAGIC
# MAGIC         #Recupera los valores de los widgets definidos anteriormente.
# MAGIC         sr_tipo_mov = dbutils.widgets.get('sr_tipo_mov')
# MAGIC         sr_proceso = dbutils.widgets.get('sr_proceso')
# MAGIC         sr_subproceso = dbutils.widgets.get('sr_subproceso')
# MAGIC         sr_origen_arc = dbutils.widgets.get('sr_origen_arc')
# MAGIC         sr_dt_org_arc = dbutils.widgets.get('sr_dt_org_arc')
# MAGIC         sr_fecha_acc = dbutils.widgets.get('sr_fecha_acc')        
# MAGIC         sr_subetapa = dbutils.widgets.get('sr_subetapa')
# MAGIC         sr_folio = dbutils.widgets.get('sr_folio')
# MAGIC         sr_folio_rel = dbutils.widgets.get('sr_folio_rel')
# MAGIC         sr_reproceso = dbutils.widgets.get('sr_reproceso')
# MAGIC         sr_aeim = dbutils.widgets.get('sr_aeim')
# MAGIC         sr_instancia_proceso = dbutils.widgets.get('sr_instancia_proceso')
# MAGIC         sr_usuario = dbutils.widgets.get('sr_usuario')
# MAGIC         sr_etapa = dbutils.widgets.get('sr_etapa')
# MAGIC         sr_id_snapshot = dbutils.widgets.get('sr_id_snapshot')
# MAGIC
# MAGIC         #Verifica si alguno de los valores recuperados está vacío o es nulo
# MAGIC         #Si algún valor está vacío, registra un error y retorna una tupla de ceros
# MAGIC         if any(len(str(value).strip()) == 0 for value in [sr_tipo_mov, sr_proceso, sr_subproceso, sr_origen_arc, sr_dt_org_arc, sr_fecha_acc, sr_subetapa, sr_folio, sr_folio_rel, sr_reproceso, sr_aeim, sr_instancia_proceso, sr_usuario, sr_etapa, sr_id_snapshot]):    
# MAGIC             logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC             logger.error("Some of the input values are empty or null")
# MAGIC             return '0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0'
# MAGIC     #Captura cualquier excepción que ocurra durante la ejecución del bloque try
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: %s", str(e))
# MAGIC         return '0','0','0','0','0','0','0','0','0','0','0','0','0','0','0','0'
# MAGIC     #Si no hay errores, retorna los valores recuperados y un indicador de éxito ('1')
# MAGIC     return sr_tipo_mov, sr_proceso, sr_subproceso, sr_origen_arc, sr_dt_org_arc, sr_fecha_acc, sr_subetapa, sr_folio, sr_folio_rel, sr_reproceso, sr_aeim, sr_instancia_proceso, sr_usuario, sr_etapa, sr_id_snapshot, '1'
# MAGIC
# MAGIC
# MAGIC #El código principal se ejecuta cuando el script se ejecuta directamente (no cuando se importa como un módulo).
# MAGIC if __name__ == "__main__" :
# MAGIC     #Configura el logger para registrar mensajes de depuración y errores.
# MAGIC     logging.basicConfig()
# MAGIC     logger = logging.getLogger(__name__)
# MAGIC     logger.setLevel(logging.DEBUG)
# MAGIC     #Obtiene el nombre del notebook y define variables para mensajes de error y la ruta del repositorio raíz.
# MAGIC     notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
# MAGIC     message = 'NB Error: ' + notebook_name
# MAGIC     source = 'ETL'
# MAGIC     root_repo = '/Workspace/Shared/MITAFO'
# MAGIC     
# MAGIC     #Intenta importar funciones adicionales desde otros notebooks. Si ocurre un error, lo registra.
# MAGIC     try:
# MAGIC         sys.path.append(root_repo + '/' + 'CGRLS_0010/Notebooks')
# MAGIC         from NB_GRLS_DML_FUNCTIONS import *
# MAGIC         from NB_GRLS_SIMPLE_FUNCTIONS import *
# MAGIC     except Exception as e:
# MAGIC         logger.error("Error at the beggining of the process")
# MAGIC         logger.error("An error was raised: " + str(e)) 
# MAGIC     
# MAGIC     #Define las rutas de los archivos de configuración necesarios para el proceso.
# MAGIC     config_file = root_repo + '/' + 'CGRLS_0010/Conf/CF_GRLS_PROCESS.py.properties'
# MAGIC     config_conn_file = root_repo + '/' + 'CGRLS_0010/Conf/CF_GRLS_CONN.py.properties'
# MAGIC     config_process_file = root_repo + '/' + 'ANCIN_0030/Jobs/03_MATRIZ_CONVIVENCIA/01_DISPERSIONES/Conf/CF_PART_PROC.py.properties'
# MAGIC
# MAGIC     #Obtiene todos los parámetros de entrada desde los widgets de Databricks.
# MAGIC     input_parameters = dbutils.widgets.getAll().items()
# MAGIC     
# MAGIC     #Llama a la función conf_init_values para recuperar los valores de configuración inicial. Si hay un error, registra el mensaje, envía una notificación y lanza una excepción.
# MAGIC     process_name = 'root'
# MAGIC     webhook_url, channel, failed_task = conf_init_values(config_file, process_name,'MCV')
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     #Llama a la función input_values para recuperar los valores de entrada. Si hay un error, registra el mensaje y lanza una excepción.
# MAGIC     sr_tipo_mov, sr_proceso, sr_subproceso, sr_origen_arc, sr_dt_org_arc, sr_fecha_acc, sr_subetapa, sr_folio, sr_folio_rel, sr_reproceso, sr_aeim, sr_instancia_proceso, sr_usuario, sr_etapa, sr_id_snapshot, failed_task = input_values()
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("An error ocurred, check out log messages")
# MAGIC

# COMMAND ----------

stage = 0

if sr_aeim != 1 and sr_tipo_mov.upper() == 'ABONO' and (sr_subproceso == '277' or sr_subproceso == '839') and sr_reproceso != '1' :
    stage = "BLOQUEO SUBSECUENTE"
elif sr_tipo_mov.upper() == 'ABONO' and ( sr_subproceso == '277'  or sr_subproceso == '839' ) and sr_reproceso == '1' :
    stage = "BLOQUEO SUBSECUENTE REPROCESO"
elif sr_aeim != 1 and sr_tipo_mov.upper() == 'ABONO' and sr_subproceso != '277' and sr_subproceso != '839' and sr_subproceso != '101' and sr_reproceso != '1' :
    stage = "ABONO"
elif sr_tipo_mov.upper() == 'ABONO' and sr_subproceso != '277' and sr_subproceso != '839' and sr_subproceso != '101' and sr_reproceso == '1' :
    stage = "ABONO REPROCESO"
elif sr_aeim != 1 and sr_tipo_mov.upper() == 'CARGO' and sr_subproceso != '101' and sr_reproceso != '1' :
    stage = "CARGO"
elif sr_tipo_mov.upper() == 'CARGO' and sr_subproceso != '101' and sr_reproceso == '1' :
    stage = "CARGO REPROCESO"

dbutils.jobs.taskValues.set(key = "sr_stage", value = stage)
dbutils.jobs.taskValues.set(key = "sr_aeim", value = sr_aeim)
dbutils.jobs.taskValues.set(key = "sr_dt_org_arc", value = sr_dt_org_arc)
dbutils.jobs.taskValues.set(key = "sr_fecha_acc", value = sr_fecha_acc)
dbutils.jobs.taskValues.set(key = "sr_folio", value = sr_folio)
dbutils.jobs.taskValues.set(key = "sr_folio_rel", value = sr_folio_rel)
dbutils.jobs.taskValues.set(key = "sr_origen_arc", value = sr_origen_arc)
dbutils.jobs.taskValues.set(key = "sr_proceso", value = sr_proceso)
dbutils.jobs.taskValues.set(key = "sr_reproceso", value = sr_reproceso)
dbutils.jobs.taskValues.set(key = "sr_subetapa", value = sr_subetapa)
dbutils.jobs.taskValues.set(key = "sr_subproceso", value = sr_subproceso)
dbutils.jobs.taskValues.set(key = "sr_tipo_mov", value = sr_tipo_mov)
dbutils.jobs.taskValues.set(key = "sr_instancia_proceso", value = sr_instancia_proceso)
dbutils.jobs.taskValues.set(key = "sr_usuario", value = sr_usuario)
dbutils.jobs.taskValues.set(key = "sr_etapa", value = sr_etapa)
dbutils.jobs.taskValues.set(key = "sr_id_snapshot", value = sr_id_snapshot)
