import pandas as pd 
import re 
import ftfy
import sys
import csv 
import pypyodbc as odbc
import subprocess

def corregir_columnas_DISTRIBUIDORAS(dfd):
    nuevos_nombres = {
        0: 'COD_DISTRIBUIDORA',
        1: 'RUC_DISTRIBUIDORA',
        2: 'RAZ_SOCIAL_DISTRIBUIDORA',
        3: 'NOMBRE_DISTRIBUIDORA',
        4: 'REGION_DISTRIBUIDORA',
        5: 'DEPARTAMENTO_DISTRIBUIDORA',
        6: 'DISTRITO_DISTRIBUIDORA',
        7: 'ESTADO_DISTRIBUIDORA'
    }
    
    # Renombra las columnas usando los índices y los nuevos nombres
    for col_index, new_nombre in nuevos_nombres.items():
        ant_nombre = dfd.columns[col_index]
        dfd = dfd.rename(columns={ant_nombre: new_nombre})
    print('Renombró correctamente las columnas')

    # for col in df.select_dtypes(include=['object']).columns : 
    #      df[col] = df[col].fillna('N/A')   
    #      df[col] = df[col].str.replace(r'NULL', 'N/A', regex=True)    
    # print('Se CORRIGIÓ NULOS') 

    for col in dfd.select_dtypes(include=['string']).columns : 
         dfd[col] = dfd[col].fillna('N/A')   
         dfd[col] = dfd[col].str.replace(r'NULL', 'N/A', regex=True)    
    print('Se CORRIGIÓ NULOS EN STRING') 

    # for col in df.select_dtypes(include=['float64']).columns: 
    #     df[col] = df[col].fillna(0.0)   
    #     df[col] = df[col].astype('int64')
    # print('Se cambió a Int') 

    # for col in df.select_dtypes(include=['int64']).columns: 
    #     df[col] = df[col].astype('string')
    #     df[col] = df[col].str.replace(r'0', 'N/A', regex=True)  
    # print('Se cambió a string')  

    for col in dfd.select_dtypes(include=['string']).columns: 
        dfd[col] = dfd[col].apply(ftfy.fix_text) 
    print('Corrigió decodificacion') 

    for col in dfd.select_dtypes(include=['object']).columns : 
        dfd[col] = dfd[col].str.replace(r'[!?-]', '', regex=True)    
    print('Se eliminó caracteres especiales')    

    indices_dfd = dfd.index
    dfd_sin_duplicados = dfd.drop_duplicates(subset=['COD_DISTRIBUIDORA'], keep='first')
    filas_eliminadas_pos =indices_dfd.difference(dfd_sin_duplicados.index)  
    cant_eliminados = len(filas_eliminadas_pos)
    if not filas_eliminadas_pos.empty : 
        print(f"Se eliminaron los registros duplicados en las posiciones: {filas_eliminadas_pos.tolist()}") 
        print(f"Se encontraron  :  {cant_eliminados} registros duplicados.")
    else: 
        print("No se encontraron duplicados.")
    
    dfd = dfd_sin_duplicados

    return dfd 

def corregir_columnas_VENDEDORES(dfv):
    nuevos_nombres = {
        0: 'COD_DISTRIBUIDORA',
        1: 'COD_VENDEDOR',
        2: 'NOMBRE_VENDEDOR',
        3: 'TIPO_DOC_VENDEDOR',
        4: 'NUM_DOC_VENDEDOR',
        5: 'CANAL_VENDEDOR',
        6: 'COD_SUPERVISOR',
        7: 'NOMBRE_SUPERVISOR',
        8: 'CODIGO_GRUPO'
    }
    
    # Renombra las columnas usando los índices y los nuevos nombres
    for col_index, new_nombre in nuevos_nombres.items():
        ant_nombre = dfv.columns[col_index]
        dfv = dfv.rename(columns={ant_nombre: new_nombre})
    print('Renombró correctamente las columnas')

    # for col in df.select_dtypes(include=['object']).columns : 
    #      df[col] = df[col].fillna('N/A')   
    #      df[col] = df[col].str.replace(r'NULL', 'N/A', regex=True)    
    # print('Se CORRIGIÓ NULOS') 

    for col in dfv.select_dtypes(include=['string']).columns : 
         dfv[col] = dfv[col].fillna('N/A')   
         dfv[col] = dfv[col].str.replace(r'NULL', 'N/A', regex=True)    
    print('Se CORRIGIÓ NULOS EN STRING') 

    # for col in df.select_dtypes(include=['float64']).columns: 
    #     df[col] = df[col].fillna(0.0)   
    #     df[col] = df[col].astype('int64')
    # print('Se cambió a Int') 

    # for col in df.select_dtypes(include=['int64']).columns: 
    #     df[col] = df[col].astype('string')
    #     df[col] = df[col].str.replace(r'0', 'N/A', regex=True)  
    # print('Se cambió a string')  

    for col in dfv.select_dtypes(include=['string']).columns: 
        dfv[col] = dfv[col].apply(ftfy.fix_text) 
    print('Corrigió decodificacion') 

    for col in dfv.select_dtypes(include=['object']).columns : 
         dfv[col] = dfv[col].str.replace(r'[!?-]', '', regex=True)    
    print('Se eliminó caracteres especiales')    
    

    # Muestra la información del DataFrame resultante
    #df.info()
    indices_dfv = dfv.index
    dfv_sin_duplicados = dfv.drop_duplicates(subset=['COD_DISTRIBUIDORA', 'COD_VENDEDOR'], keep='first')
    filas_eliminadas_pos =indices_dfv.difference(dfv_sin_duplicados.index)  
    cant_eliminados = len(filas_eliminadas_pos)
    if not filas_eliminadas_pos.empty : 
        print(f"Se eliminaron los registros duplicados en las posiciones: {filas_eliminadas_pos.tolist()}") 
        print(f"Se encontraron  :  {cant_eliminados} registros duplicados.")
    else: 
        print("No se encontraron duplicados.")
    
    dfv = dfv_sin_duplicados
    return dfv 

def corregir_columnas_CLIENTES(dfc):    
    nuevos_nombres = {
        0: 'COD_DISTRIBUIDORA',
        1: 'COD_CLIENTE',
        2: 'COD_VENDEDOR',
        3: 'NOMBRE_CLIENTE',
        4: 'TIPO_DOC_CLIENTE',
        5: 'NUM_DOC_CLIENTE',
        6: 'TIPO_NEGOCIO_CLIENTE',
        7: 'DISTRITO_CLIENTE',
        8: 'RUTA_ENTREGA',
        9: 'ESTADO_CLIENTE'
    }
    
    # Renombra las columnas usando los índices y los nuevos nombres
    for col_index, new_nombre in nuevos_nombres.items():
        ant_nombre = dfc.columns[col_index]
        dfc = dfc.rename(columns={ant_nombre: new_nombre})
    print('Renombró correctamente las columnas')

    # for col in df.select_dtypes(include=['object']).columns : 
    #      df[col] = df[col].fillna('N/A')   
    #      df[col] = df[col].str.replace(r'NULL', 'N/A', regex=True)    
    # print('Se CORRIGIÓ NULOS') 

    for col in dfc.select_dtypes(include=['string']).columns : 
         dfc[col] = dfc[col].fillna('N/A')   
         dfc[col] = dfc[col].str.replace(r'NULL', 'N/A', regex=True)    
    print('Se CORRIGIÓ NULOS EN STRING') 

    # for col in df.select_dtypes(include=['float64']).columns: 
    #     df[col] = df[col].fillna(0.0)   
    #     df[col] = df[col].astype('int64')
    # print('Se cambió a Int') 

    # for col in df.select_dtypes(include=['int64']).columns: 
    #     df[col] = df[col].astype('string')
    #     df[col] = df[col].str.replace(r'0', 'N/A', regex=True)  
    # print('Se cambió a string')  

    for col in dfc.select_dtypes(include=['string']).columns: 
        dfc[col] = dfc[col].apply(ftfy.fix_text) 
    print('Corrigió decodificacion') 

    for col in dfc.select_dtypes(include=['object']).columns : 
        dfc[col] = dfc[col].str.replace(r'[!?-]', '', regex=True)    
    print('Se eliminó caracteres especiales')    

    indices_dfc = dfc.index
    dfc_sin_duplicados = dfc.drop_duplicates(subset=['COD_DISTRIBUIDORA', 'COD_CLIENTE', 'COD_VENDEDOR'], keep='first')
    filas_eliminadas_pos =indices_dfc.difference(dfc_sin_duplicados.index) 
    cant_eliminados = len(filas_eliminadas_pos)
    if not filas_eliminadas_pos.empty : 
        print(f"Se eliminaron los registros duplicados en las posiciones: {filas_eliminadas_pos.tolist()}") 
        print(f"Se encontraron  :  {cant_eliminados} registros duplicados.")

    else: 
        print("No se encontraron duplicados.")
    

    # Muestra la información del DataFrame resultante
    #df.info()
    dfc = dfc_sin_duplicados
    return dfc 


def corregir_columnas_PRODUCTOS(dfp):    
    nuevos_nombres = {
        0: 'COD_DISTRIBUIDORA',
        1: 'COD_PRODUCTO',
        2: 'DESC_PRODUCTO',
        3: 'COD_EAN',
        4: 'NOM_PROVEEDOR',
        5: 'FACTOR_UNI_MIN',
        6: 'FACTOR_UNI_MAX'
    }
    
    # Renombra las columnas usando los índices y los nuevos nombres
    for col_index, new_nombre in nuevos_nombres.items():
        ant_nombre = dfp.columns[col_index]
        dfp = dfp.rename(columns={ant_nombre: new_nombre})
    print('Renombró correctamente las columnas')

    for col in dfp.select_dtypes(include=['object']).columns: 
         dfp[col] = dfp[col].fillna('N/A')   
         dfp[col] = dfp[col].astype('string')
         dfp[col] = dfp[col].str.replace(r'NULL', 'N/A', regex=True) 
    print('Se cambió a String y corrigió nulos') 

    # for col in dfp.select_dtypes(include=['string']).columns : 
    #      dfp[col] = dfp[col].fillna('N/A')   
    #      dfp[col] = dfp[col].str.replace(r'NULL', 'N/A', regex=True)    
    # print('Se CORRIGIÓ NULOS EN STRING') 
    for col in dfp.select_dtypes(include=['string']).columns: 
        dfp[col] = dfp[col].apply(ftfy.fix_text) 
    print('Corrigió decodificacion') 

    for col in dfp.select_dtypes(include=['string']).columns : 
        dfp[col] = dfp[col].str.replace(r'[!?-]', '', regex=True)    
    print('Se eliminó caracteres especiales')    

    indices_dfp = dfp.index
    dfp_sin_duplicados = dfp.drop_duplicates(subset=['COD_DISTRIBUIDORA', 'COD_PRODUCTO'], keep='first')
    filas_eliminadas_pos =indices_dfp.difference(dfp_sin_duplicados.index)  
    cant_eliminados = len(filas_eliminadas_pos)
    if not filas_eliminadas_pos.empty : 
        print(f"Se eliminaron los registros duplicados en las posiciones: {filas_eliminadas_pos.tolist()}") 
        print(f"Se encontraron  :  {cant_eliminados} registros duplicados.")
    else: 
        print("No se encontraron duplicados.")
    
    dfp = dfp_sin_duplicados
    return dfp 




def corregir_columnas_EQUIVALENCIAS(dfe):    
    nuevos_nombres = {
        0: 'COD_DISTRIBUIDORA',
        1: 'COD_PROD_DISTRIBUIDOR',
        2: 'COD_PROD_MAESTRO',
        3: 'FECHA_REGISTRO',
        4: 'USUARIO'
    }
    
    # Renombra las columnas usando los índices y los nuevos nombres
    for col_index, new_nombre in nuevos_nombres.items():
        ant_nombre = dfe.columns[col_index]
        dfe = dfe.rename(columns={ant_nombre: new_nombre})
    print('Renombró correctamente las columnas')

    # for col in df.select_dtypes(include=['object']).columns : 
    #      df[col] = df[col].fillna('N/A')   
    #      df[col] = df[col].str.replace(r'NULL', 'N/A', regex=True)    
    # print('Se CORRIGIÓ NULOS') 

    for col in dfe.select_dtypes(include=['string']).columns : 
         dfe[col] = dfe[col].fillna('N/A')   
         dfe[col] = dfe[col].str.replace(r'NULL', 'N/A', regex=True)    
    print('Se CORRIGIÓ NULOS EN STRING') 

    # for col in df.select_dtypes(include=['float64']).columns: 
    #     df[col] = df[col].fillna(0.0)   
    #     df[col] = df[col].astype('int64')
    # print('Se cambió a Int') 

    # for col in df.select_dtypes(include=['int64']).columns: 
    #     df[col] = df[col].astype('string')
    #     df[col] = df[col].str.replace(r'0', 'N/A', regex=True)  
    # print('Se cambió a string')  

    for col in dfe.select_dtypes(include=['string']).columns: 
        dfe[col] = dfe[col].apply(ftfy.fix_text) 
    print('Corrigió decodificacion') 

    for col in dfe.select_dtypes(include=['object']).columns : 
        dfe[col] = dfe[col].str.replace(r'[!?-]', '', regex=True)    
    print('Se eliminó caracteres especiales')    

    indices_dfe = dfe.index
    dfe_sin_duplicados = dfe.drop_duplicates(subset=['COD_DISTRIBUIDORA', 'COD_PROD_DISTRIBUIDOR','COD_PROD_MAESTRO'], keep='first')
    filas_eliminadas_pos =indices_dfe.difference(dfe_sin_duplicados.index)  
    cant_eliminados = len(filas_eliminadas_pos)
    if not filas_eliminadas_pos.empty : 
        print(f"Se eliminaron los registros duplicados en las posiciones: {filas_eliminadas_pos.tolist()}") 
        print(f"Se encontraron  :  {cant_eliminados} registros duplicados.")
    else: 
        print("No se encontraron duplicados.")
    
    dfe = dfe_sin_duplicados
    return dfe 


def corregir_columnas_VENTAS(dfcvent):    
    nuevos_nombres = {
        0: 'COD_DISTRIBUIDORA',
        1: 'COD_VENTA',
        2: 'NUM_FACTURA',
        3: 'FECHA',
        4: 'COD_CLIENTE',
        5: 'COD_ITEM',
        6: 'COD_PRODUCTO', 
        7: 'CANTIDAD_UND_MIN', 
        8: 'TIPO_UND_MIN' ,
        9: 'CANT_UND_MAX',
        10: 'TIPO_UND_MAX',
        11: 'VENTA_SINIGV',
        12: 'VENTA_CONIGV',
        13: 'TIPO_PRODUCTO',
        14: 'COD_VENDEDOR'

    }
    
    # Renombra las columnas usando los índices y los nuevos nombres
    for col_index, new_nombre in nuevos_nombres.items():
        ant_nombre = dfcvent.columns[col_index]
        dfcvent = dfcvent.rename(columns={ant_nombre: new_nombre})
    print('Renombró correctamente las columnas')

    # for col in df.select_dtypes(include=['object']).columns : 
    #      df[col] = df[col].fillna('N/A')   
    #      df[col] = df[col].str.replace(r'NULL', 'N/A', regex=True)    
    # print('Se CORRIGIÓ NULOS') 

    for col in dfcvent.select_dtypes(include=['object']).columns: 
         dfcvent[col] = dfcvent[col].fillna('N/A')   
         dfcvent[col] = dfcvent[col].astype('string')
         dfcvent[col] = dfcvent[col].str.replace(r'NULL', 'N/A', regex=True) 
    print('Se cambió a String y corrigió nulos') 

    # for col in dfcvent.select_dtypes(include=['string']).columns : 
    #      dfcvent[col] = dfcvent[col].fillna('N/A')   
    #      dfcvent[col] = dfcvent[col].str.replace(r'NULL', 'N/A', regex=True)    
    # print('Se CORRIGIÓ NULOS EN STRING') 

    # for col in df.select_dtypes(include=['float64']).columns: 
    #     df[col] = df[col].fillna(0.0)   
    #     df[col] = df[col].astype('int64')
    # print('Se cambió a Int') 

    # for col in df.select_dtypes(include=['int64']).columns: 
    #     df[col] = df[col].astype('string')
    #     df[col] = df[col].str.replace(r'0', 'N/A', regex=True)  
    # print('Se cambió a string')  

    for col in dfcvent.select_dtypes(include=['string']).columns: 
        dfcvent[col] = dfcvent[col].apply(ftfy.fix_text) 
    print('Corrigió decodificacion') 

    for col in dfcvent.select_dtypes(include=['string']).columns : 
        dfcvent[col] = dfcvent[col].str.replace(r'[!?-]', '', regex=True)    
    print('Se eliminó caracteres especiales')    

    indices_dfcvent = dfcvent.index
    dfcvent_sin_duplicados = dfcvent.drop_duplicates(subset=['COD_DISTRIBUIDORA', 'NUM_FACTURA','COD_CLIENTE','COD_ITEM','COD_PRODUCTO','COD_VENDEDOR'], keep='first')
    filas_eliminadas_pos =indices_dfcvent.difference(dfcvent_sin_duplicados.index)  
    cant_eliminados = len(filas_eliminadas_pos)
    if not filas_eliminadas_pos.empty : 
        print(f"Se eliminaron los registros duplicados en las posiciones: {filas_eliminadas_pos.tolist()}") 
        print(f"Se encontraron  :  {cant_eliminados} registros duplicados.")
    else: 
        print("No se encontraron duplicados.")
    
    dfcvent = dfcvent_sin_duplicados
    return dfcvent 




def corregir_columnas_MAESTRO_GENERAL(dfm):    
    nuevos_nombres = {
        0: 'COD_PROD',
        1: 'DESC_PROD',
        2: 'EAN_13',
        3: 'CATEGORIA_PROD',
        4: 'FAMILIA_PROD',
        5: 'MARCA_PROD',
        6: 'SEGMENTO_PROD',
        7: 'UND_X_CAJA',
        8: 'PESO_X_UND',
        9: 'PRESENTACION_PROD',
        10: 'ESTADO_PROD'
    }
    
    # Renombra las columnas usando los índices y los nuevos nombres
    for col_index, new_nombre in nuevos_nombres.items():
        ant_nombre = dfm.columns[col_index]
        dfm = dfm.rename(columns={ant_nombre: new_nombre})
    print('Renombró correctamente las columnas')

    for col in dfm.select_dtypes(include=['object']).columns: 
         dfm[col] = dfm[col].fillna(0.0)   
         dfm[col] = dfm[col].astype('string')
    print('Se cambió a String') 
    

    for col in dfm.select_dtypes(include=['string']).columns : 
         dfm[col] = dfm[col].fillna('N/A')   
         dfm[col] = dfm[col].str.replace(r'NULL', 'N/A', regex=True)    
    print('Se CORRIGIÓ NULOS EN STRING') 
   
    for col in dfm.select_dtypes(include=['string']).columns: 
        dfm[col] = dfm[col].apply(ftfy.fix_text) 
    print('Corrigió decodificacion') 

    for col in dfm.select_dtypes(include=['string']).columns : 
        dfm[col] = dfm[col].str.replace(r'[!?-]', '', regex=True)    
    print('Se eliminó caracteres especiales')    

    indices_dfm = dfm.index
    dfm_sin_duplicados = dfm.drop_duplicates(subset=['COD_PROD'], keep='first')
    filas_eliminadas_pos =indices_dfm.difference(dfm_sin_duplicados.index)  
    cant_eliminados = len(filas_eliminadas_pos)
    if not filas_eliminadas_pos.empty : 
        print(f"Se eliminaron los registros duplicados en las posiciones: {filas_eliminadas_pos.tolist()}") 
        print(f"Se encontraron  :  {cant_eliminados} registros duplicados.")
    else: 
        print("No se encontraron duplicados.")
    
    dfm = dfm_sin_duplicados
    return dfm 




def detectar_separador(filepath):
    with open(filepath, 'r', encoding='utf-8') as file:
        # Lee las primeras 1024 bytes del archivo para detectar el separador
        sample = file.read(1024)
        file.seek(0)  # Regresa el puntero al inicio del archivo

        # Detecta el delimitador más probable usando Sniffer
        dialect = csv.Sniffer().sniff(sample)

    return dialect.delimiter


def detectar_separador_montos(filepath):
    with open(filepath, 'r', encoding='unicode-escape') as file:
        # Lee las primeras 1024 bytes del archivo para detectar el separador
        sample = file.read(1024)
        file.seek(0)  # Regresa el puntero al inicio del archivo

        # Detecta el delimitador más probable usando Sniffer
        dialect = csv.Sniffer().sniff(sample)

    return dialect.delimiter
 
def db_connector(): 
    DRIVER_NAME = 'SQL Server' 
    SERVER_NAME = 'FREYA' 
    DATABASE_NAME = 'DB_CARGA_D' 
 
    connection_string = 'DRIVER={'+DRIVER_NAME+'};SERVER='+SERVER_NAME+';DATABASE='+DATABASE_NAME+';Trusted_Connection=yes'
    return connection_string



def ejecutar_scripts(script_nom):
    try:
        print(f"Ejecutando el script: {script_nom}")
        subprocess.run(['python', script_nom], check=True)
        print(f"El script {script_nom} se ejecutó correctamente.")
    except subprocess.CalledProcessError as e:
        print(f"Ocurrió un error al ejecutar el script: {e}")
    except FileNotFoundError:
        print(f"El archivo {script_nom} no se encontró en la carpeta actual.")