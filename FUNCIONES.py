import pandas as pd 
import re 
import ftfy
import sys
import csv 
import pypyodbc as odbc

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

    return dfv 

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

    return dfd 


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

    # for col in df.select_dtypes(include=['object']).columns : 
    #      df[col] = df[col].fillna('N/A')   
    #      df[col] = df[col].str.replace(r'NULL', 'N/A', regex=True)    
    # print('Se CORRIGIÓ NULOS') 

    for col in dfp.select_dtypes(include=['string']).columns : 
         dfp[col] = dfp[col].fillna('N/A')   
         dfp[col] = dfp[col].str.replace(r'NULL', 'N/A', regex=True)    
    print('Se CORRIGIÓ NULOS EN STRING') 

    # for col in df.select_dtypes(include=['float64']).columns: 
    #     df[col] = df[col].fillna(0.0)   
    #     df[col] = df[col].astype('int64')
    # print('Se cambió a Int') 

    # for col in df.select_dtypes(include=['int64']).columns: 
    #     df[col] = df[col].astype('string')
    #     df[col] = df[col].str.replace(r'0', 'N/A', regex=True)  
    # print('Se cambió a string')  

    for col in dfp.select_dtypes(include=['string']).columns: 
        dfp[col] = dfp[col].apply(ftfy.fix_text) 
    print('Corrigió decodificacion') 

    for col in dfp.select_dtypes(include=['object']).columns : 
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




def corregir_columnas_EQUIVALENCIAS(dfven):    
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
    dfe_sin_duplicados = dfe.drop_duplicates(subset=['COD_DISTRIBUIDORA', 'COD_PRODUCTO'], keep='first')
    filas_eliminadas_pos =indices_dfe.difference(dfe_sin_duplicados.index)  
    cant_eliminados = len(filas_eliminadas_pos)
    if not filas_eliminadas_pos.empty : 
        print(f"Se eliminaron los registros duplicados en las posiciones: {filas_eliminadas_pos.tolist()}") 
        print(f"Se encontraron  :  {cant_eliminados} registros duplicados.")
    else: 
        print("No se encontraron duplicados.")
    
    dfe = dfe_sin_duplicados
    return dfe 


def corregir_columnas_VENTAS(dfven):    
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
        ant_nombre = dfven.columns[col_index]
        dfven = dfven.rename(columns={ant_nombre: new_nombre})
    print('Renombró correctamente las columnas')

    # for col in df.select_dtypes(include=['object']).columns : 
    #      df[col] = df[col].fillna('N/A')   
    #      df[col] = df[col].str.replace(r'NULL', 'N/A', regex=True)    
    # print('Se CORRIGIÓ NULOS') 

    for col in dfven.select_dtypes(include=['string']).columns : 
         dfven[col] = dfven[col].fillna('N/A')   
         dfven[col] = dfven[col].str.replace(r'NULL', 'N/A', regex=True)    
    print('Se CORRIGIÓ NULOS EN STRING') 

    # for col in df.select_dtypes(include=['float64']).columns: 
    #     df[col] = df[col].fillna(0.0)   
    #     df[col] = df[col].astype('int64')
    # print('Se cambió a Int') 

    # for col in df.select_dtypes(include=['int64']).columns: 
    #     df[col] = df[col].astype('string')
    #     df[col] = df[col].str.replace(r'0', 'N/A', regex=True)  
    # print('Se cambió a string')  

    for col in dfven.select_dtypes(include=['string']).columns: 
        dfven[col] = dfven[col].apply(ftfy.fix_text) 
    print('Corrigió decodificacion') 

    for col in dfven.select_dtypes(include=['object']).columns : 
        dfven[col] = dfven[col].str.replace(r'[!?-]', '', regex=True)    
    print('Se eliminó caracteres especiales')    

    indices_dfven = dfven.index
    dfven_sin_duplicados = dfven.drop_duplicates(subset=['COD_DISTRIBUIDORA', 'COD_PRODUCTO'], keep='first')
    filas_eliminadas_pos =indices_dfven.difference(dfven_sin_duplicados.index)  
    cant_eliminados = len(filas_eliminadas_pos)
    if not filas_eliminadas_pos.empty : 
        print(f"Se eliminaron los registros duplicados en las posiciones: {filas_eliminadas_pos.tolist()}") 
        print(f"Se encontraron  :  {cant_eliminados} registros duplicados.")
    else: 
        print("No se encontraron duplicados.")
    
    dfven = dfven_sin_duplicados
    return dfven 

def detectar_separador(filepath):
    with open(filepath, 'r', encoding='utf-8') as file:
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