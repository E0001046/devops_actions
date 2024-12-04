# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC %reload_ext autoreload
# MAGIC
# MAGIC import sys
# MAGIC import logging
# MAGIC from pyspark.sql.functions import col
# MAGIC from pyspark.sql import SparkSession
# MAGIC
# MAGIC # Configuración del logger
# MAGIC # Configuración personalizada del formato del logger
# MAGIC log_format = "%(asctime)s - %(levelname)s - %(message)s - [Archivo: %(filename)s, Línea: %(lineno)d]"
# MAGIC
# MAGIC # Configuración del logger
# MAGIC logging.getLogger().setLevel(logging.INFO)
# MAGIC logger = logging.getLogger("py4j")
# MAGIC logger.setLevel(logging.WARN)
# MAGIC logger = logging.getLogger(__name__)
# MAGIC
# MAGIC prod = True  # Para setear paths
# MAGIC
# MAGIC # Variables globales
# MAGIC
# MAGIC root_repo = "/Workspace/Shared/MITAFO"
# MAGIC config_files = {
# MAGIC     "general": f"{root_repo}/CGRLS_0010/Conf/CF_GRLS_PROCESS.py.properties",
# MAGIC     "connection": f"{root_repo}/CGRLS_0010/Conf/CF_GRLS_CONN.py.properties",
# MAGIC     "process": f"{root_repo}/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/DISPERSIONES/Conf/CF_PART_PROC.py.properties"
# MAGIC     if prod
# MAGIC     else "/Workspace/Repos/mronboye@emeal.nttdata.com/QueryConfigLab.ide/"
# MAGIC     "MITAFO/ANCIN_0030/Jobs/04_GEN_ACRED_MOVS/"
# MAGIC     "DISPERSIONES/Conf/"
# MAGIC     "CF_PART_PROC.py.properties",
# MAGIC }
# MAGIC
# MAGIC notebook_name = (
# MAGIC     dbutils.notebook.entry_point.getDbutils()
# MAGIC     .notebook()
# MAGIC     .getContext()
# MAGIC     .notebookPath()
# MAGIC     .get()
# MAGIC )
# MAGIC message = "NB Error: " + notebook_name
# MAGIC source = "ETL"
# MAGIC
# MAGIC process_name = "root"
# MAGIC
# MAGIC # Carga de funciones externas
# MAGIC sys.path.append(f"{root_repo}/CGRLS_0010/Notebooks")
# MAGIC try:
# MAGIC     from NB_GRLS_DML_FUNCTIONS import *
# MAGIC     from NB_GRLS_SIMPLE_FUNCTIONS import *
# MAGIC except Exception as e:
# MAGIC     logger.error("Error al cargar funciones externas: %s", e)
# MAGIC
# MAGIC global_params = {}
# MAGIC global_confs = {}  # Diccionario para almacenar las keys globales
# MAGIC
# MAGIC
# MAGIC def input_values() -> dict:
# MAGIC     """Obtiene los valores de los widgets de entrada y los almacena en un diccionario global."""
# MAGIC     widget_defaults = {
# MAGIC         "SR_FOLIO": "",
# MAGIC         "SR_FOLIO_REL": "",
# MAGIC         "SR_PROCESO": "",
# MAGIC         "SR_SUBPROCESO": "",
# MAGIC         "SR_SUBETAPA": "",
# MAGIC         "SR_ORIGEN_ARC": "",
# MAGIC         "SR_FECHA_ACC": "",
# MAGIC         "SR_FECHA_LIQ": "",
# MAGIC         "SR_TIPO_MOV": "",
# MAGIC         "SR_REPROCESO": "",
# MAGIC         "SR_FCC_USU_CRE": "EJE_CIERRENDATA",
# MAGIC         "SR_FLC_USU_REG": "EJE_CIERRENDATA",
# MAGIC         "CX_CRE_ESQUEMA": "CIERREN_ETL",
# MAGIC         "TL_CRE_DISPERSION": "TTSISGRAL_ETL_DISPERSION",
# MAGIC         "SR_INSTANCIA_PROCESO": "",
# MAGIC         "SR_USUARIO": "",
# MAGIC         "SR_ID_SNAPSHOT": "",
# MAGIC     }
# MAGIC
# MAGIC     # Crear los widgets en minúsculas
# MAGIC     for key, default_value in widget_defaults.items():
# MAGIC         dbutils.widgets.text(key.lower(), default_value)
# MAGIC
# MAGIC     # Actualizar el diccionario global en mayúsculas
# MAGIC     global_params.update(
# MAGIC         {
# MAGIC             key.upper(): dbutils.widgets.get(key.lower()).strip()
# MAGIC             for key in widget_defaults
# MAGIC         }
# MAGIC     )
# MAGIC
# MAGIC     if any(not value for value in global_params.values()):
# MAGIC         logger.error("Valores de entrada vacíos o nulos")
# MAGIC         global_params["status"] = "0"
# MAGIC     else:
# MAGIC         global_params["status"] = "1"
# MAGIC
# MAGIC     return global_params
# MAGIC
# MAGIC
# MAGIC # Configuración del manejador global de excepciones
# MAGIC def global_exception_handler(exc_type, exc_value, exc_traceback):
# MAGIC     if issubclass(exc_type, KeyboardInterrupt):
# MAGIC         # Permitir que KeyboardInterrupt se maneje normalmente
# MAGIC         sys.__excepthook__(exc_type, exc_value, exc_traceback)
# MAGIC         return
# MAGIC
# MAGIC     message = f"Uncaught exception: {exc_value}"
# MAGIC     source = "ETL"
# MAGIC     input_parameters = dbutils.widgets.getAll().items()
# MAGIC     # Registro del error y notificación
# MAGIC     logger.error("Please review log messages")
# MAGIC     notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC     raise Exception("An error raised")
# MAGIC
# MAGIC
# MAGIC # Asigna el manejador de excepciones al hook global de sys
# MAGIC sys.excepthook = global_exception_handler
# MAGIC
# MAGIC
# MAGIC if __name__ == "__main__":
# MAGIC     # Inicialización de variables
# MAGIC     input_parameters = dbutils.widgets.getAll().items()
# MAGIC
# MAGIC     webhook_url, channel, failed_task = conf_init_values(
# MAGIC         config_files["general"], process_name, "TEMP_PROCESS"
# MAGIC     )
# MAGIC     input_values()
# MAGIC     if global_params["status"] == "0":
# MAGIC         logger.error("Revisar mensajes en los logs")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Error en los valores de entrada, revisar logs")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Cambio de valor del parametro `SR_TIPO_MOV`**
# MAGIC
# MAGIC Originalmente BUS envia el parametro `SR_TIPO_MOV` con el valor de 'ABONO' o 'CARGO'. Este se debe cambiar a su id respectivo.

# COMMAND ----------

# Normalizar el valor de SR_TIPO_MOV a mayúsculas
global_params["SR_TIPO_MOV"] = global_params["SR_TIPO_MOV"].upper()

if global_params["SR_TIPO_MOV"] == "ABONO" or global_params["SR_TIPO_MOV"] == "2":
    dbutils.widgets.remove("sr_tipo_mov")  # Remover el widget existente
    global_params["SR_TIPO_MOV"] = "2"
    logger.info(
        f"El valor de SR_TIPO_MOV ha sido cambiado a {global_params['SR_TIPO_MOV']}"
    )
else:
    dbutils.widgets.remove("sr_tipo_mov")  # Remover el widget existente
    global_params["SR_TIPO_MOV"] = "1"
    logger.info(
        f"El valor de SR_TIPO_MOV ha sido cambiado a {global_params['SR_TIPO_MOV']}"
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### **Cambio del valor del parametro `SR_ETAPA` (No lo envian si no que se crea a partir de sr_resproceso)**
# MAGIC
# MAGIC Este siempre toma el valor del parametro `SR_REPROCESO`

# COMMAND ----------

global_params["SR_ETAPA"] = global_params["SR_REPROCESO"]
try:
    dbutils.widgets.remove("sr_etapa")  # Remover el widget existente
except Exception as e:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculamos el valor de Factor

# COMMAND ----------

if int(global_params["SR_ETAPA"]) in [1, 3]:
    global_params["SR_FACTOR"] = 1
else:
    global_params["SR_FACTOR"] = -1

logger.info(f"El valor de SR_FACTOR es: {global_params['SR_FACTOR']}")
try:
    dbutils.widgets.remove("sr_factor")  # Remover el widget existente
except Exception as e:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculamos `SR_FOLIO_REL`

# COMMAND ----------

if global_params["SR_FOLIO_REL"] in ["NA", "na"]:
    global_params["SR_FOLIO_REL"] = "null"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculamos `SR_FECHA_ACC`

# COMMAND ----------

if global_params["SR_FECHA_ACC"] in ["NA", "na"]:
    global_params["SR_FECHA_ACC"] = "null"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reestablecer / Crear los widgets

# COMMAND ----------

# Crear los widgets con los valores normalizados
dbutils.widgets.text("sr_tipo_mov", str(global_params["SR_TIPO_MOV"]))
dbutils.widgets.text("sr_etapa", str(global_params["SR_ETAPA"]))
dbutils.widgets.text("sr_factor", str(global_params["SR_FACTOR"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Seteamos los parametros por defecto 

# COMMAND ----------

# Anadimos el nuevo widget a la tarea
dbutils.jobs.taskValues.set(key="sr_etapa", value=global_params["SR_ETAPA"])
dbutils.jobs.taskValues.set(key="sr_fcc_usu_cre", value=global_params["SR_FCC_USU_CRE"])
dbutils.jobs.taskValues.set(key="sr_flc_usu_reg", value=global_params["SR_FLC_USU_REG"])
dbutils.jobs.taskValues.set(key="sr_factor", value=global_params["SR_FACTOR"])
dbutils.jobs.taskValues.set(key="cx_cre_esquema", value=global_params["CX_CRE_ESQUEMA"])
dbutils.jobs.taskValues.set(key="tl_cre_dispersion", value=global_params["TL_CRE_DISPERSION"])
dbutils.jobs.taskValues.set(key="sr_factor", value=global_params["SR_FACTOR"])


# COMMAND ----------

# MAGIC %md
# MAGIC ### Validaciones del flujo

# COMMAND ----------

stage = 0

if (global_params["SR_SUBETAPA"] == '273') and (global_params["SR_SUBPROCESO"] != '101'):  # GENERACION DE MOVIMIENTOS
    if global_params["SR_TIPO_MOV"] == '2':  # ABONO
        if global_params["SR_ETAPA"] == '1':  # PROCESO NORMAL
            stage = "ABONO"
        elif global_params["SR_ETAPA"] == '2':  # RECHAZO
            stage = "ABONO RECHAZO"
        elif global_params["SR_ETAPA"] == '3':  # RECALCULO
            stage = "ABONO RECALCULO"
    else:  # CARGO
        if global_params["SR_ETAPA"] == '1':  # PROCESO NORMAL
            stage = "CARGO"
        elif global_params["SR_ETAPA"] == '2':  # RECHAZO
            stage = "CARGO RECHAZO"
        elif global_params["SR_ETAPA"] == '3':  # RECALCULO
            stage = "CARGO RECALCULO"
else:
    logger.error("Parametros de entrada no validos para generar movimientos")

# Obtiene todos los parámetros de entrada desde los widgets de Databricks.
input_parameters = dbutils.widgets.getAll().items()
dbutils.jobs.taskValues.set(key = "", value = stage)
logger.info(f"params: {input_parameters}")
logger.info(f"sr_stage: {stage}")

dbutils.jobs.taskValues.set(key="sr_stage", value=stage)
dbutils.jobs.taskValues.set(key="sr_fecha_acc", value=global_params["SR_FECHA_ACC"])
dbutils.jobs.taskValues.set(key="sr_fecha_liq", value=global_params["SR_FECHA_LIQ"])
dbutils.jobs.taskValues.set(key="sr_folio", value=global_params["SR_FOLIO"])
dbutils.jobs.taskValues.set(key="sr_folio_rel", value=global_params["SR_FOLIO_REL"])
dbutils.jobs.taskValues.set(key="sr_origen_arc", value=global_params["SR_ORIGEN_ARC"])
dbutils.jobs.taskValues.set(key="sr_proceso", value=global_params["SR_PROCESO"])
dbutils.jobs.taskValues.set(key="sr_reproceso", value=global_params["SR_REPROCESO"])
dbutils.jobs.taskValues.set(key="sr_subetapa", value=global_params["SR_SUBETAPA"])
dbutils.jobs.taskValues.set(key="sr_subproceso", value=global_params["SR_SUBPROCESO"])
dbutils.jobs.taskValues.set(key="sr_tipo_mov", value=global_params["SR_TIPO_MOV"])
dbutils.jobs.taskValues.set(key="sr_instancia_proceso", value=global_params["SR_INSTANCIA_PROCESO"])
dbutils.jobs.taskValues.set(key="sr_usuario", value=global_params["SR_USUARIO"])
dbutils.jobs.taskValues.set(key="sr_id_snapshot", value=global_params["SR_ID_SNAPSHOT"])


