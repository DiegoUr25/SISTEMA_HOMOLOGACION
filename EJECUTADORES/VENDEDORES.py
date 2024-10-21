import pandas as pd 
import sys
import ftfy as f
from datetime import *
from funciones import *
import os

# filepath_output =r'C:/Users/PC - Usuario/Desktop/TESIS/ARCHIVOS/ARCHIVOS_PROCESADOS'
# filepath_input = 'C:/Users/PC - Usuario/Desktop/PRUEBAS_ETL/TABLAS/Vendedores.csv' 

# Camino al directorio raíz desde la ubicación del script actual
root_directory = os.path.dirname(os.path.dirname(__file__))

# Construye las rutas hacia las carpetas input y output en el directorio raíz
filepath_input = os.path.join(root_directory, 'input/Vendedores.csv')
filepath_output = os.path.join(root_directory, 'output')

# print (filepath_input, filepath_output)

class Vendedores(): 
          def __init__(self,COD_DISTRIBUIDORA,COD_VENDEDOR,	NOMBRE_VENDEDOR,	TIPO_DOC_VENDEDOR,	NUM_DOC_VENDEDOR	,CANAL_VENDEDOR	,COD_SUPERVISOR	,NOMBRE_SUPERVISOR,	CODIGO_GRUPO): 
               self.COD_DISTRIBUIDORA = COD_DISTRIBUIDORA 
               self.COD_VENDEDOR = COD_VENDEDOR
               self.NOMBRE_VENDEDOR = NOMBRE_VENDEDOR
               self.TIPO_DOC_VENDEDOR = TIPO_DOC_VENDEDOR
               self.NUM_DOC_VENDEDOR = NUM_DOC_VENDEDOR
               self.CANAL_VENDEDOR = CANAL_VENDEDOR
               self.COD_SUPERVISOR = COD_SUPERVISOR
               self.NOMBRE_SUPERVISOR = NOMBRE_SUPERVISOR
               self.CODIGO_GRUPO = CODIGO_GRUPO 
    
          def Grabar(self, dfv): 
               conexion=odbc.connect(db_connector())   
               cursor = conexion.cursor() 

               try: 
                    cursor.execute("DELETE FROM CLIENTES")
                    cursor.execute("BEGIN TRANSACTION")
                    for index, row in dfv.iterrows():  
                         self.COD_DISTRIBUIDORA = row['COD_DISTRIBUIDORA'] 
                         self.COD_VENDEDOR = row['COD_VENDEDOR']
                         self.NOMBRE_VENDEDOR = row['NOMBRE_VENDEDOR']
                         self.TIPO_DOC_VENDEDOR = row['TIPO_DOC_VENDEDOR']
                         self.NUM_DOC_VENDEDOR = row['NUM_DOC_VENDEDOR']
                         self.CANAL_VENDEDOR = row['CANAL_VENDEDOR']
                         self.COD_SUPERVISOR = row['COD_SUPERVISOR']
                         self.NOMBRE_SUPERVISOR = row['NOMBRE_SUPERVISOR']
                         self.CODIGO_GRUPO = row['CODIGO_GRUPO'] 


                         Insert="""INSERT INTO VENDEDORES (COD_DISTRIBUIDORA, COD_VENDEDOR, NOMBRE_VENDEDOR, TIPO_DOC_VENDEDOR, NUM_DOC_VENDEDOR, CANAL_VENDEDOR, COD_SUPERVISOR, NOMBRE_SUPERVISOR, CODIGO_GRUPO) 
                                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"""
          
                         cursor.execute(Insert,(
                              self.COD_DISTRIBUIDORA, 
                              self.COD_VENDEDOR, 
                              self.NOMBRE_VENDEDOR, 
                              self.TIPO_DOC_VENDEDOR, 
                              self.NUM_DOC_VENDEDOR, 
                              self.CANAL_VENDEDOR, 
                              self.COD_SUPERVISOR, 
                              self.NOMBRE_SUPERVISOR, 
                              self.CODIGO_GRUPO
                              ))
                         conexion.commit() 
               except Exception as e: 
                     conexion.rollback()
                     print(f"Error al grabar Vendedores: {e}")
               finally:
                     print('CARGÓ CORRECTAMENTE VENDEDORES A BD')
                     conexion.close()



separador = detectar_separador(filepath_input) 
fecha_actual = datetime.now().strftime("%Y%m%d_%H%M%S")

if filepath_input.endswith('.csv'): 
     dfv= pd.read_csv(filepath_input, delimiter = separador, encoding='unicode_escape',dtype='string') 
     num_columnas = dfv.shape[1]  
     print ("Tiene la siguiente cant de columnas : ", num_columnas) 

     #dfv.info()
     ###TRANSFORMACION 
     #VALIDACIÓN DE FORMATO DE COLUMNAS ADMITIDAS PARA EL PROCESO 


     print(dfv.head(100)) 
     print(dfv.info())
     if num_columnas == 9 :
          dfv= corregir_columnas_VENDEDORES(dfv) 
          print(dfv.head(98))
          print("Todo ok") 

          
               

          TESTINSTANCE = Vendedores(None, None, None, None, None, None, None, None, None)
          TESTINSTANCE.Grabar(dfv)
          nombre_tabla="VENDEDORES"
          nombre_output = f"{filepath_output}\\{nombre_tabla}_{fecha_actual}.csv" 

          dfv.to_csv(nombre_output, index=False)
          print(f"Archivo procesado guardado como {nombre_output}")
     
        
     else : 
          print("Faltan columnas , se detiene el proceso.") 
          sys.exit() 
elif filepath_input.endswith('.txt'):
     dfv= pd.read_csv(filepath_input, delimiter = separador, encoding='unicode_escape',dtype='string') 
     num_columnas = dfv.shape[1]  
     print ("Tiene la siguiente cant de columnas : ", num_columnas) 

     #dfv.info()
     ###TRANSFORMACION 
     #VALIDACIÓN DE FORMATO DE COLUMNAS ADMITIDAS PARA EL PROCESO 


     print(dfv.head(100)) 
     print(dfv.info())
     if num_columnas == 9 :
          dfv= corregir_columnas_VENDEDORES(dfv) 
          print(dfv.head(98))
          print("Todo ok") 

          
               

          TESTINSTANCE = Vendedores(None, None, None, None, None, None, None, None, None)
          TESTINSTANCE.Grabar(dfv)
          nombre_tabla="VENDEDORES"
          nombre_output = f"{filepath_output}\\{nombre_tabla}_{fecha_actual}.txt" 

          dfv.to_csv(nombre_output,sep=';' ,index=False)
          print(f"Archivo procesado guardado como {nombre_output}") 
     else : 
          print("Faltan columnas , se detiene el proceso.") 
          sys.exit() 
elif filepath_input.endswith('.xslx'): 
     dfv= pd.read_excel(filepath_input, sheet_name=0, engine='openpyxl',dtype='string') 
     num_columnas = dfv.shape[1]  
     print ("Tiene la siguiente cant de columnas : ", num_columnas) 

     #dfv.info()
     ###TRANSFORMACION 
     #VALIDACIÓN DE FORMATO DE COLUMNAS ADMITIDAS PARA EL PROCESO 


     print(dfv.head(100)) 
     print(dfv.info())
     if num_columnas == 9 :
          dfv= corregir_columnas_VENDEDORES(dfv) 
          print(dfv.head(98))
          print("Todo ok") 

          
               

          TESTINSTANCE = Vendedores(None, None, None, None, None, None, None, None, None)
          TESTINSTANCE.Grabar(dfv)
          nombre_tabla="VENDEDORES"
          nombre_output = f"{filepath_output}\\{nombre_tabla}_{fecha_actual}.csv" 

          dfv.to_csv(nombre_output, index=False)
          print(f"Archivo procesado guardado como {nombre_output}") 
     else : 
          print("Faltan columnas , se detiene el proceso.") 
          sys.exit() 
else : 
      print("Formato de archivo no permitido. /n")
      print("Los formatos permitidos son .txt, .csv y .xslx")
      sys.exit()

