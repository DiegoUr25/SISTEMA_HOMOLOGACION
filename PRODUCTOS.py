import pandas as pd 
import sys
import ftfy as f
from datetime import *
from FUNCIONES import *

filepath_output =r'C:/Users/PC - Usuario/Desktop/TESIS/ARCHIVOS/ARCHIVOS_PROCESADOS'
filepath_input = 'C:/Users/PC - Usuario/Desktop/PRUEBAS_ETL/TABLAS/Productos.csv' 
# separador = detectar_separador(filepath_input) 
fecha_actual = datetime.now().strftime("%Y%m%d_%H%M%S")

dfp= pd.read_csv(filepath_input, delimiter = ';', encoding='unicode_escape') 
num_columnas = dfp.shape[1]  
print ("Tiene la siguiente cant de columnas : ", num_columnas) 

#dfp.info()
###TRANSFORMACION 
#VALIDACIÓN DE FORMATO DE COLUMNAS ADMITIDAS PARA EL PROCESO 


print(dfp.head(100)) 
print(dfp.info())
if num_columnas == 7 :
     dfp= corregir_columnas_PRODUCTOS(dfp) 
     print(dfp.head(98))
     print("Todo ok") 

     class Productos(): 
          def __init__(self,COD_DISTRIBUIDORA,COD_PRODUCTO,	DESC_PRODUCTO,	COD_EAN,	NOM_PROVEEDOR	,FACTOR_UNI_MIN	,FACTOR_UNI_MAX	): 
               self.COD_DISTRIBUIDORA = COD_DISTRIBUIDORA 
               self.COD_PRODUCTO = COD_PRODUCTO
               self.DESC_PRODUCTO = DESC_PRODUCTO
               self.COD_EAN = COD_EAN
               self.NOM_PROVEEDOR = NOM_PROVEEDOR
               self.FACTOR_UNI_MIN = FACTOR_UNI_MIN
               self.FACTOR_UNI_MAX = FACTOR_UNI_MAX
               
    
          def Grabar(self, dfp): 
               conexion=odbc.connect(db_connector())   
               cursor = conexion.cursor() 
               
               try: 
                    cursor.execute("BEGIN TRANSACTION") 
                    for index, row in dfp.iterrows():  
                         self.COD_DISTRIBUIDORA = row['COD_DISTRIBUIDORA'] 
                         self.COD_PRODUCTO = row['COD_PRODUCTO']
                         self.DESC_PRODUCTO = row['DESC_PRODUCTO']
                         self.COD_EAN = row['COD_EAN']
                         self.NOM_PROVEEDOR = row['NOM_PROVEEDOR']
                         self.FACTOR_UNI_MIN = row['FACTOR_UNI_MIN']
                         self.FACTOR_UNI_MAX = row['FACTOR_UNI_MAX']
                    


                         Insert="""INSERT INTO PRODUCTOS (COD_DISTRIBUIDORA, COD_PRODUCTO, DESC_PRODUCTO, COD_EAN, NOM_PROVEEDOR, FACTOR_UNI_MIN, FACTOR_UNI_MAX) 
                                   VALUES (?, ?, ?, ?, ?, ?, ?)"""
          
                         cursor.execute(Insert,(
                              self.COD_DISTRIBUIDORA, 
                              self.COD_PRODUCTO, 
                              self.DESC_PRODUCTO, 
                              self.COD_EAN, 
                              self.NOM_PROVEEDOR, 
                              self.FACTOR_UNI_MIN, 
                              self.FACTOR_UNI_MAX
                              ))
                         conexion.commit() 


               except Exception as e: 
                         conexion.rollback()
                         print(f"Error al grabar Distribuidoras: {e}")
               finally:
                         print('CARGÓ CORRECTAMENTE PRODUCTOS A BD')
                         conexion.close()
          
     TESTINSTANCE = Productos(None, None, None, None, None, None, None)
     TESTINSTANCE.Grabar(dfp)
     nombre_tabla="PRODUCTOS"
     nombre_output = f"{filepath_output}\\{nombre_tabla}_{fecha_actual}.csv" 

     dfp.to_csv(nombre_output, index=False)
     print(f"Archivo procesado guardado como {nombre_output}")
     
        
else : 
         print("Faltan columnas , se detiene el proceso.") 
         sys.exit() 



