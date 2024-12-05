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
# MAGIC import uuid
# MAGIC
# MAGIC from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
# MAGIC from pyspark.sql.functions import length, lit
# MAGIC
# MAGIC def input_values():
# MAGIC     """ Recupera valores de entrada desde widgets.
# MAGIC     Returns:
# MAGIC         tuple: Una tupla que contiene los valores de entrada y un indicador de estado.
# MAGIC     """
# MAGIC     try:
# MAGIC         # Define widgets de texto en Databricks para capturar valores de entrada.
# MAGIC         dbutils.widgets.text('sr_folio', '')
# MAGIC         dbutils.widgets.text('sr_proceso', '')
# MAGIC         dbutils.widgets.text('sr_subproceso', '')
# MAGIC         dbutils.widgets.text('sr_subetapa', '')
# MAGIC         dbutils.widgets.text('sr_tipo_mov', '')
# MAGIC         dbutils.widgets.text('sr_accion', '')
# MAGIC
# MAGIC         # Recupera los valores de los widgets definidos anteriormente.
# MAGIC         sr_folio = dbutils.widgets.get('sr_folio')
# MAGIC         sr_proceso = dbutils.widgets.get('sr_proceso')
# MAGIC         sr_subproceso = dbutils.widgets.get('sr_subproceso')
# MAGIC         sr_subetapa = dbutils.widgets.get('sr_subetapa')
# MAGIC         sr_tipo_mov = dbutils.widgets.get('sr_tipo_mov')
# MAGIC         sr_accion = dbutils.widgets.get('sr_accion')
# MAGIC
# MAGIC         # Verifica si alguno de los valores recuperados está vacío o es nulo.
# MAGIC         # Si algún valor está vacío, registra un error y retorna una tupla de ceros.
# MAGIC         if any(len(str(value).strip()) == 0 for value in [sr_folio, sr_proceso, sr_subproceso, sr_subetapa, sr_tipo_mov, sr_accion]):
# MAGIC             logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC             logger.error("Some of the input values are empty or null")
# MAGIC             return '0', '0', '0', '0', '0', '0'
# MAGIC     
# MAGIC     # Captura cualquier excepción que ocurra durante la ejecución del bloque try.
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: %s", str(e))
# MAGIC         return '0', '0', '0', '0', '0', '0', '0'
# MAGIC     return sr_folio, sr_proceso, sr_subproceso, sr_subetapa, sr_tipo_mov, sr_accion, '1'
# MAGIC
# MAGIC
# MAGIC def conf_process_values(config_file, process_name):
# MAGIC     try:
# MAGIC         # Inicializa un objeto ConfigParser y lee el archivo de configuración especificado por config_file.
# MAGIC         config = configparser.ConfigParser()
# MAGIC         config.read(config_file)
# MAGIC
# MAGIC         # Configuración de subprocesos.
# MAGIC         # Recupera los valores de configuración específicos para el proceso indicado por process_name.
# MAGIC         sql_conf_file = config.get(process_name, 'sql_conf_file')
# MAGIC     # Captura cualquier excepción que ocurra durante la ejecución del bloque try.
# MAGIC     except Exception as e:
# MAGIC         logger.error("Function: %s", inspect.stack()[0][3])
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC         return '0', '0'
# MAGIC     return sql_conf_file, '1'
# MAGIC
# MAGIC # El código principal se ejecuta cuando el script se ejecuta directamente (no cuando se importa como un módulo).
# MAGIC if __name__ == "__main__":
# MAGIC     logging.basicConfig()
# MAGIC     logger = logging.getLogger(__name__)
# MAGIC     logger.setLevel(logging.DEBUG)
# MAGIC     notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
# MAGIC     message = 'NB Error: ' + notebook_name
# MAGIC     source = 'ETL'
# MAGIC     root_repo = '/Workspace/Shared/MITAFO'
# MAGIC     
# MAGIC     # Intenta importar funciones adicionales desde otros notebooks. Si ocurre un error, lo registra.
# MAGIC     try:
# MAGIC         sys.path.append(root_repo + '/' + 'CGRLS_0010/Notebooks')
# MAGIC         from NB_GRLS_DML_FUNCTIONS import *
# MAGIC         from NB_GRLS_SIMPLE_FUNCTIONS import *
# MAGIC     except Exception as e:
# MAGIC         logger.error("Error at the beggining of the process")
# MAGIC         logger.error("An error was raised: " + str(e))
# MAGIC
# MAGIC     # Define las rutas de los archivos de configuración necesarios para el proceso.
# MAGIC     config_file = root_repo + '/' + 'CGRLS_0010/Conf/CF_GRLS_PROCESS.py.properties'
# MAGIC     config_conn_file = root_repo + '/' + 'CGRLS_0010/Conf/CF_GRLS_CONN.py.properties'
# MAGIC     config_process_file = root_repo + '/' + 'ANCIN_0030/Jobs/05_CONCLUSION/03_DESMAR_CUENTA/Conf/CF_PART_PROC.py.properties'
# MAGIC
# MAGIC     # Llama a la función input_values para recuperar los valores de entrada. Si hay un error, registra el mensaje y lanza una excepción.
# MAGIC     sr_folio, sr_proceso, sr_subproceso, sr_subetapa, sr_tipo_mov, sr_accion, failed_task = input_values()
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         #notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("An error ocurred, check out log messages")
# MAGIC
# MAGIC     # Obtiene todos los parámetros de entrada desde los widgets de Databricks.
# MAGIC     input_parameters = dbutils.widgets.getAll().items()
# MAGIC     
# MAGIC     # Llama a la función conf_init_values para recuperar los valores de configuración inicial. Si hay un error, registra el mensaje, envía una notificación y lanza una excepción.
# MAGIC     process_name = 'root'
# MAGIC     webhook_url, channel, failed_task = conf_init_values(config_file, process_name, 'DESMARCA')
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     # Llama a la función conf_process_values para recuperar los valores de configuración del proceso. Si hay un error, registra el mensaje, envía una notificación y lanza una excepción.
# MAGIC     process_name = 'root'
# MAGIC     sql_conf_file, failed_task = conf_process_values(config_process_file, process_name)
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     # Llama a la función conf_conn_values para recuperar los valores de configuración de conexión. Si hay un error, registra el mensaje, envía una notificación y lanza una excepción.
# MAGIC     conn_name_ora = 'jdbc_oracle'
# MAGIC     conn_options, conn_aditional_options, conn_user, conn_key, conn_url, scope, failed_task = conf_conn_values(config_conn_file, conn_name_ora)
# MAGIC     if failed_task == '0':
# MAGIC         logger.error("Please review log messages")
# MAGIC         notification_raised(webhook_url, -1, message, source, input_parameters)
# MAGIC         raise Exception("Process ends")
# MAGIC
# MAGIC     # Definición de la ruta del archivo de configuración SQL.
# MAGIC     sql_conf_file = root_repo + '/' + '/ANCIN_0030/Jobs/05_CONCLUSION/03_DESMAR_CUENTA/JSON/' + sql_conf_file

# COMMAND ----------

# DBTITLE 1,CONFIGURACIÓN ARCHIVO SQL Y JSON
# Abre el archivo de configuración SQL y carga su contenido en formato JSON
with open(sql_conf_file) as f:
    file_config_sql = json.load(f)

# Procesa el contenido del archivo JSON para extraer los valores de configuración
# Crea una lista de renglones donde cada renglón contiene el 'step_id' y los valores concatenados por salto de línea
conf_values = [
    (fields['step_id'], '\n'.join(fields['value']))  # Crea una tupla con 'step_id' y los valores concatenados
    for line, value in file_config_sql.items() if line == 'steps'  # Itera sobre los elementos del JSON y filtra por 'steps'
    for fields in value  # Itera sobre los campos dentro de 'steps'
]

# COMMAND ----------

# MAGIC %md
# MAGIC Falta agregar la toma de decisión en WF: **$SR_ACCION=0 OR $SR_ACCION=1 OR $SR_ACCION=3  - Desmarca** **$SR_TIPO_MOV='CANCELA_FOLIO' AND $SR_ACCION=3 - Cancelación de Folio**

# COMMAND ----------

# MAGIC %md
# MAGIC **El Notebook hace el camino de Cancelación de Folio**

# COMMAND ----------

# DBTITLE 1,VALIDACIÓN DE EXISTENCIA DE FOLIO
# Define el identificador de la consulta
query_statement = '004'

# Parámetros para la consulta
params = [sr_folio]

# Recupera la declaración SQL usando el identificador y los parámetros
statement, failed_task = getting_statement(conf_values, query_statement, params)

# Verifica si la recuperanción de la declaración SQL falló.
if failed_task == '0':
    logger.error("No value %s found", statement)
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("Process ends")

# Ejecuta la consulta y recupera el resultado como un DataFrame
df, failed_task = query_table(conn_name_ora, spark, statement, conn_options, conn_user, conn_key)

# Verifica si la ejecución de la consulta falló.
if failed_task == '0':
    logger.error("Please review log messages")
    notification_raised(webhook_url, -1, message, source, input_parameters)
    raise Exception("An error raised")

from pyspark.sql.types import IntegerType

# Asignar el tamaño de la columna TOTAL como integer 1
df_total = df.withColumn("TOTAL", df["TOTAL"].cast(IntegerType()))

# Muestra el DataFrame creado con el nombre df_total
display(df_total)


# COMMAND ----------

# DBTITLE 1,BORRADO DE DATOS EN DIVERSIF_CONC
#JOB 0020_CANCELA_FOLIO: Realiza el borrado de datos en dos tablas (PROFAPOVOL.TTAFOAVOL_DIVERSIF_CONC y CIERREN_ETL.TTSISGRAL_ETL_BANCOS_DOMI, también debe realizar una actualización a la tabla PROFAPOVOL.TTAFOAVOL_REPLICA_DIVERSIF_SOL; para lo cuál se utiliza un DELETE/INSERT en vez de un MERGE )


# Leer la columna TOTAL y preguntar si es mayor o igual a 0
df_total_folio = df_total.select("TOTAL").collect()[0][0]

if df_total_folio >= 0: #Si el valor de la columna TOTAL es mayor o igual a cero debe realizar las siguientes tareas:

    #ELIMINA los registros de la tabla PROFAPOVOL.TTAFOAVOL_DIVERSIF_CONC
    df_delete_diversif = f"""
        DELETE FROM 
        PROFAPOVOL.TTAFOAVOL_DIVERSIF_CONC
        WHERE FTN_ID_CONCILIACION IN (
        SELECT FTN_ID_CONCILIACION FROM PROFAPOVOL.TTAFOAVOL_CONCILIACION
        WHERE FTC_FOLIO='{sr_folio}')
    """
    #ELIMINA los registros de la tabla PROFAPOVOL.TTSISGRAL_ETL_BANCOS_DOMI
    df_delete_bancos = f"""
        DELETE FROM  
        CIERREN_ETL.TTSISGRAL_ETL_BANCOS_DOMI
        WHERE FTC_FOLIO='{sr_folio}')
    """

    # Ejecuta la consulta de eliminación y recupera el resultado como un DataFrame
    df1, failed_task = query_table(conn_name_ora, spark, df_delete_diversif, conn_options, conn_user, conn_key)
    df2, failed_task = query_table(conn_name_ora, spark, df_delete_bancos, conn_options, conn_user, conn_key)

    #Antes de borrar recupera los registros de la tabla PROFAPOVOL.TTAFOAVOL_REPLICA_DIVERSIF_SOL con la finalidad de guardarlos en un dataframe para que puedan ser utilizados más tarde como un insert. Con esto se evita la parte de realizar un MERGE; la instrucción original realmente hace un UPDATE a la tabla.
    df_diversif_sol = f"""
            SELECT
                FTC_CVE_SOLICITUD,
                FTN_FONDO_APOVOL,
                FTN_MONTO_DIV,
                FTN_PORCENTAJE,
                NULL AS FTN_MONTO_REC,
                NULL AS FTN_ID_ARCHIVO,
                FTC_ID_CARGO,
                FTN_ID_PRIORIDAD,
                FTD_FEH_CRE,
                FTC_USU_CRE,
                NULL AS FTD_FEH_ACT,
                NULL AS FTC_USU_ACT,
                FLN_ID_ARCHIVO_DOMI
            FROM PROFAPOVOL.TTAFOAVOL_REPLICA_DIVERSIF_SOL
            WHERE ftc_id_cargo IN ( 
            SELECT ftc_id_cargo
                FROM PROFAPOVOL.TTCRXGRAL_CARGOS
                WHERE  ftc_folio_archivo = '{sr_folio}')
    """
    # Ejecuta la consulta de seleccionar y recupera el resultado en un DataFrame
    df_diversif_sol_insert, failed_task = query_table(conn_name_ora, spark, df_diversif_sol, conn_options, conn_user, conn_key)

    display(df_diversif_sol_insert)

    #ELIMINA los registros de la tabla TTAFOAVOL_REPLICA_DIVERSIF_SOL
    df_delete_diversif= f"""
        DELETE FROM
        PROFAPOVOL.TTAFOAVOL_REPLICA_DIVERSIF_SOL
        WHERE ftc_id_cargo IN ( 
        SELECT ftc_id_cargo
        FROM PROFAPOVOL.TTCRXGRAL_CARGOS
        WHERE  ftc_folio_archivo ='{sr_folio}')
        """
    # Ejecuta la consulta de eliminación y recupera el resultado como un DataFrame
    df3, failed_task = query_table(conn_name_ora, spark, df_delete_diversif, conn_options, conn_user, conn_key)

    #JOB 0020_CANCELA_FOLIO
    #Inserta los registros que se guardaron en el dataframe de selección con el nombre df_diversif_sol_insert en la tabla PROFAPOVOL.TTAFOAVOL_REPLICA_DIVERSIF_SOL.

    mode = 'APPEND'  # Modo de inserción: 'APPEND' para agregar nuevos registros sin eliminar los existentes

    table_name_001 = 'PROFAPOVOL.TTAFOAVOL_REPLICA_DIVERSIF_SOL' # Nombre de la tabla de destino en la base de datos
    failed_task = write_into_table(conn_name_ora, df_diversif_sol_insert, mode, table_name_001, conn_options, conn_aditional_options, conn_user, conn_key)
    # Llama a la función write_into_table para insertar los datos del dataframe en la tabla especificada

    if failed_task == '0':  # Verifica si la tarea de inserción falló (failed_task == '0' indica fallo)
        logger.error("Please review log messages")  # Registra un mensaje de error en el log
        notification_raised(webhook_url, -1, message, source, input_parameters)  # Envía una notificación de error
        raise Exception("An error raised")  # Lanza una excepción para detener la ejecución del código

else:
    #Si no se cumple la condición de arriba, el TOTAL del dataframe, debe imprimir este mensaje 
    print('El FOLIO ' + sr_folio + ' no corresponde a una domiciliación o Domiciliación traspaso')

# COMMAND ----------

# DBTITLE 1,ACTUALIZACIÓN DE INFORMACIÓN A NULOS Y DELETE
#JOB JP_PANCIN_0030_CANCELA_FOLIO. Actualiza a valores nulos dos tablas (PROFAPOVOL.TTCRXGRAL_CARGOS y PROFAPOVOL.TTCRXGRAL_DETALLE_CARGO)

# Leer la columna TOTAL y preguntar si es mayor o igual a 0
df_total_folio = df_total.select("TOTAL").collect()[0][0]

if df_total_folio >= 0: #Si el valor de la columna TOTAL es mayor o igual a cero debe realizar las siguientes tareas:

    df_select_cargos = f"""
        SELECT 
            FTC_ID_CARGO,
            FTC_CVE_SOLICITUD,
            FTN_NUM_CTA_INVDUAL,
            FTN_IMPORTE_SOLICITUD,
            NULL AS FTN_IMPORTE_REC,
            NULL AS FTC_FOLIO_ARCHIVO,
            FTN_ESTATUS_CARGO,
            NULL AS FTD_FEH_CARGO_BANCO,
            NULL AS FCN_ID_RECHAZO_BANCO,
            FTN_PARCIALIDAD,
            FLN_ID_ARCHIVO_DOMI,
            FTD_FEH_GEN_CARGO,
            FTN_REINTENTO,
            FTD_FEH_PROX_CARGO,
            FTD_FEH_CRE,
            FTC_USU_CRE,
            NULL AS FTD_FEH_ACT,
            NULL AS FTC_USU_ACT
        FROM PROFAPOVOL.TTCRXGRAL_CARGOS
        WHERE FTC_FOLIO_ARCHIVO='{sr_folio}'
    """
    # Ejecuta la consulta de seleccionar y recupera el resultado en un DataFrame
    df_insert_cargos, failed_task = query_table(conn_name_ora, spark, df_select_cargos, conn_options, conn_user, conn_key)

    display(df_insert_cargos)

    df_select_detalle_cargo = f"""
        SELECT
            FTC_ID_CARGO,
            FTC_CVE_SOLICITUD,
            FTN_NUM_CTA_INVDUAL,
            FTN_IMPORTE_CARGO,
            FLN_ID_ARCHIVO_DOMI,
            NULL AS FTN_ESTATUS_CARGO,
            NULL AS FTC_FOLIO_ARCHIVO,
            NULL AS FTD_FEH_CARGO_BANCO,
            NULL AS FTN_ID_RECHAZO_BANCO,
            FTN_NUM_PARCIALIDAD,
            FTN_NO_LINEA,
            NULL AS FTN_ID_ARCHIVO,
            FTD_FEH_GEN_CARGO,
            FTD_FEH_CARGO,
            FTD_FEH_CRE,
            FTC_USU_CRE,
            NULL AS FTD_FEH_ACT,
            NULL AS FTC_USU_ACT
        FROM PROFAPOVOL.TTCRXGRAL_DETALLE_CARGO
        WHERE FTC_FOLIO_ARCHIVO='{sr_folio}'
    """
    # Ejecuta la consulta de seleccionar y recupera el resultado en un DataFrame
    df_insert_detalle_cargo, failed_task = query_table(conn_name_ora, spark, df_select_detalle_cargo, conn_options, conn_user, conn_key)

    display(df_insert_detalle_cargo)


    #ELIMINA los registros de la tabla PROFAPOVOL.TTCRXGRAL_CARGOS
    df_delete_cargos= f"""
        DELETE FROM
        PROFAPOVOL.TTCRXGRAL_CARGOS
        WHERE FTC_FOLIO_ARCHIVO='{sr_folio}'
        """
    # Ejecuta la consulta de eliminación y recupera el resultado como un DataFrame
    df4, failed_task = query_table(conn_name_ora, spark, df_delete_cargos, conn_options, conn_user, conn_key)

    #ELIMINA los registros de la tabla PROFAPOVOL.TTCRXGRAL_DETALLE_CARGO
    df_delete_detalle_cargo= f"""
        DELETE FROM
        PROFAPOVOL.TTCRXGRAL_DETALLE_CARGO
        WHERE FTC_FOLIO_ARCHIVO='{sr_folio}'
        """
    # Ejecuta la consulta de eliminación y recupera el resultado como un DataFrame
    df5, failed_task = query_table(conn_name_ora, spark, df_delete_detalle_cargo, conn_options, conn_user, conn_key)

    #Inserta los registros que se guardaron en los dataframes df_select_cargos y df_select_detalle_cargo para insertarlos en sus tablas correspondientes.

    mode = 'APPEND'  # Modo de inserción: 'APPEND' para agregar nuevos registros sin eliminar los existentes

    table_name_002 = 'PROFAPOVOL.TTCRXGRAL_CARGOS' # Nombre de la tabla de destino en la base de datos
    failed_task = write_into_table(conn_name_ora, df_insert_cargos, mode, table_name_002, conn_options, conn_aditional_options, conn_user, conn_key)
    # Llama a la función write_into_table para insertar los datos del dataframe en la tabla especificada

    table_name_003 = 'PROFAPOVOL.TTCRXGRAL_DETALLE_CARGO' # Nombre de la tabla de destino en la base de datos
    failed_task = write_into_table(conn_name_ora, df_insert_detalle_cargo, mode, table_name_003, conn_options, conn_aditional_options, conn_user, conn_key)
    # Llama a la función write_into_table para insertar los datos del dataframe en la tabla especificada

    if failed_task == '0':  # Verifica si la tarea de inserción falló (failed_task == '0' indica fallo)
        logger.error("Please review log messages")  # Registra un mensaje de error en el log
        notification_raised(webhook_url, -1, message, source, input_parameters)  # Envía una notificación de error
        raise Exception("An error raised")  # Lanza una excepción para detener la ejecución del código'''


    #JOB JP_PANCIN_0040_CANCELA_FOLIO.
    #Finalmente elimina los registros de la tabla PROFAPOVOL.TTAFOAVOL_CONCILIACION
    df_delete_conciliacion= f"""
        DELETE from PROFAPOVOL.TTAFOAVOL_CONCILIACION
        WHERE FTC_FOLIO='{sr_folio}'
        AND FTN_ORIGEN_APORTACION in (
        845,--Domiciliación Traspasos
        291,--Domiciliación
        433,--Domiciliación
        937) --Domiciliación Traspaso
        """
    # Ejecuta la consulta de eliminación y recupera el resultado como un DataFrame
    df6, failed_task = query_table(conn_name_ora, spark, df_delete_conciliacion, conn_options, conn_user, conn_key)


else:
    #Si no se cumple la condición de arriba, debe imprimir el siguiente mensaje 
    print('El FOLIO ' + sr_folio + ' no corresponde a una domiciliación o Domiciliación traspaso')
