import pandas as pd 
import sys
import ftfy as f
from datetime import *
from FUNCIONES import *


filepath_output =r'C:/Users/PC - Usuario/Desktop/TESIS/ARCHIVOS/ARCHIVOS_PROCESADOS'
filepath_input = 'C:/Users/PC - Usuario/Desktop/PRUEBAS_ETL/TABLAS/Maestro_general.csv' 
##TODO: SE CAE POR MI FUNCIÓN DE SEPARADOR DE ARCHIVOS, PRESÚNTAMENTE POR LOS CAMPOS DE MONTO O MI PC NO ENCUENTRA EL DELIMITADOR, REVISAR SI SE SOLUCIONA CON UN IF O ALGO ASÍ 
# separador = detectar_separador_montos(filepath_input) 
fecha_actual = datetime.now().strftime("%Y%m%d_%H%M%S")
dfm= pd.read_csv(filepath_input, delimiter = ';', encoding='unicode-escape') 
num_columnas = dfm.shape[1]  
print ("Tiene la siguiente cant de columnas : ", num_columnas) 

#dfm.info()
###TRANSFORMACION
#VALIDACIÓN DE FORMATO DE COLUMNAS ADMITIDAS PARA EL PROCESO 


print(dfm.head(100)) 
print(dfm.info())
if num_columnas == 11 :
     dfm= corregir_columnas_MAESTRO_GENERAL(dfm) 
     print(dfm.head(98))
     print("Todo ok") 

     class MAESTRO_GENERAL(): 
          def __init__(self,COD_PROD,DESC_PROD,	EAN_13,	CATEGORIA_PROD,	FAMILIA_PROD	,MARCA_PROD	,SEGMENTO_PROD	,UND_X_CAJA,	PESO_X_UND, PRESENTACION_PROD, ESTADO_PROD): 
               self.COD_PROD = COD_PROD 
               self.DESC_PROD = DESC_PROD
               self.EAN_13 = EAN_13
               self.CATEGORIA_PROD = CATEGORIA_PROD
               self.FAMILIA_PROD = FAMILIA_PROD
               self.MARCA_PROD = MARCA_PROD
               self.SEGMENTO_PROD = SEGMENTO_PROD
               self.UND_X_CAJA = UND_X_CAJA
               self.PESO_X_UND = PESO_X_UND 
               self.PRESENTACION_PROD = PRESENTACION_PROD 
               self.ESTADO_PROD = ESTADO_PROD
              
    
          def Grabar(self, dfm): 
               conexion=odbc.connect(db_connector())   
               cursor = conexion.cursor() 

               try: 
                    cursor.execute("BEGIN TRANSACTION")  
                    for index, row in dfm.iterrows():  
                         self.COD_PROD = row['COD_PROD'] 
                         self.DESC_PROD = row['DESC_PROD']
                         self.EAN_13 = row['EAN_13']
                         self.CATEGORIA_PROD = row['CATEGORIA_PROD']
                         self.FAMILIA_PROD = row['FAMILIA_PROD']
                         self.MARCA_PROD = row['MARCA_PROD']
                         self.SEGMENTO_PROD = row['SEGMENTO_PROD']
                         self.UND_X_CAJA = row['UND_X_CAJA']
                         self.PESO_X_UND = row['PESO_X_UND'] 
                         self.PRESENTACION_PROD = row['PRESENTACION_PROD'] 
                         self.ESTADO_PROD = row['ESTADO_PROD'] 
                         


                         Insert="""INSERT INTO MAESTRO_GENERAL (COD_PROD, DESC_PROD, EAN_13, CATEGORIA_PROD, FAMILIA_PROD, MARCA_PROD, SEGMENTO_PROD, UND_X_CAJA, PESO_X_UND, PRESENTACION_PROD,ESTADO_PROD) 
                                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
          
                         cursor.execute(Insert,(
                               self.COD_PROD, 
                               self.DESC_PROD, 
                               self.EAN_13, 
                               self.CATEGORIA_PROD, 
                               self.FAMILIA_PROD, 
                               self.MARCA_PROD, 
                               self.SEGMENTO_PROD, 
                               self.UND_X_CAJA, 
                               self.PESO_X_UND, 
                               self.PRESENTACION_PROD,
                               self.ESTADO_PROD
                               ))
                         conexion.commit() 
               except Exception as e: 
                              conexion.rollback() 
                              print(f"Error al grabar MAESTRO_GENERAL: {e}")
               finally: 
                    print('CARGÓ CORRECTAMENTE MAESTRO_GENERAL A BD')
                    conexion.close()
                         
                         
                             
          

     TESTINSTANCE = MAESTRO_GENERAL(None, None, None, None, None, None,None,None,None,None,None)
     TESTINSTANCE.Grabar(dfm)
     print('CARGÓ CORRECTAMENTE MAESTRO_GENERAL A BD')
     nombre_tabla="MAESTRO_GENERAL"
     nombre_output = f"{filepath_output}\\{nombre_tabla}_{fecha_actual}.csv" 
     
     dfm.to_csv(nombre_output, index = False) 
     print(f"Archivo procesado guardado como {nombre_output}")

else : 
         print("Faltan columnas , se detiene el proceso.") 
         sys.exit() 



