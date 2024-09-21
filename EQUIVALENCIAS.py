import pandas as pd 
import sys
import ftfy as f
from datetime import *
from FUNCIONES import *


filepath_output =r'C:/Users/PC - Usuario/Desktop/TESIS/ARCHIVOS/ARCHIVOS_PROCESADOS'
filepath_input = 'C:/Users/PC - Usuario/Desktop/PRUEBAS_ETL/TABLAS/Equivalencias.csv' 
separador = detectar_separador(filepath_input) 
fecha_actual = datetime.now().strftime("%Y%m%d_%H%M%S")

dfe= pd.read_csv(filepath_input, delimiter = separador, encoding='unicode_escape',dtype='string') 
num_columnas = dfe.shape[1]  
print ("Tiene la siguiente cant de columnas : ", num_columnas) 

#dfe.info()
###TRANSFORMACION 
#VALIDACIÓN DE FORMATO DE COLUMNAS ADMITIDAS PARA EL PROCESO 


print(dfe.head(100)) 
print(dfe.info())
if num_columnas == 5 :
     dfe= corregir_columnas_EQUIVALENCIAS(dfe) 
     print(dfe.head(98))
     print("Todo ok") 

     class Equivalencias(): 
          def __init__(self,COD_DISTRIBUIDORA,COD_PROD_DISTRIBUIDOR,	COD_PROD_MAESTRO,	FECHA_REGISTRO,	USUARIO): 
               self.COD_DISTRIBUIDORA = COD_DISTRIBUIDORA 
               self.COD_PROD_DISTRIBUIDOR = COD_PROD_DISTRIBUIDOR
               self.COD_PROD_MAESTRO = COD_PROD_MAESTRO
               self.FECHA_REGISTRO = FECHA_REGISTRO
               self.USUARIO = USUARIO
              
               
    
          def Grabar(self, dfe): 
               conexion=odbc.connect(db_connector())   
               cursor = conexion.cursor() 
               try: 
                    cursor.execute("BEGIN TRANSACTION")
                    for index, row in dfe.iterrows():  
                         self.COD_DISTRIBUIDORA = row['COD_DISTRIBUIDORA'] 
                         self.COD_PROD_DISTRIBUIDOR = row['COD_PROD_DISTRIBUIDOR']
                         self.COD_PROD_MAESTRO = row['COD_PROD_MAESTRO']
                         self.FECHA_REGISTRO = row['FECHA_REGISTRO']
                         self.USUARIO = row['USUARIO']
                         
                    


                         Insert="""INSERT INTO EQUIVALENCIAS (COD_DISTRIBUIDORA, COD_PROD_DISTRIBUIDOR, COD_PROD_MAESTRO, FECHA_REGISTRO, USUARIO) 
                                   VALUES (?, ?, ?, ?, ?)"""
          
                         cursor.execute(Insert,(
                              self.COD_DISTRIBUIDORA,
                              self.COD_PROD_DISTRIBUIDOR, 
                              self.COD_PROD_MAESTRO, 
                              self.FECHA_REGISTRO, 
                              self.USUARIO
                              ))
                         conexion.commit() 
               except Exception as e: 
                     conexion.rollback() 
                     print(f"Error al grabar Equivalencias: {e}")
               finally:
                     print('CARGÓ CORRECTAMENTE EQUIVALENCIAS A BD')
                     conexion.close()
          

     TESTINSTANCE = Equivalencias(None, None, None, None, None)
     TESTINSTANCE.Grabar(dfe)
     nombre_tabla="EQUIVALENCIAS"
     nombre_output = f"{filepath_output}\\{nombre_tabla}_{fecha_actual}.csv" 
     dfe.to_csv(nombre_output, index=False)
     print(f"Archivo procesado guardado como {nombre_output}")
     
        
else : 
         print("Faltan columnas , se detiene el proceso.") 
         sys.exit() 



