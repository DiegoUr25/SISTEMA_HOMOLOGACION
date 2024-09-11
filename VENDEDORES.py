
import pandas as pd 
import sys
import ftfy as f
from FUNCIONES import *

filepath = 'C:/Users/PC - Usuario/Desktop/PRUEBAS_ETL/TABLAS/Vendedores.csv' 
separador = detectar_separador(filepath) 
dfv= pd.read_csv(filepath, delimiter = separador, encoding='unicode_escape',dtype='string') 
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
         
                    cursor.execute(Insert,(self.COD_DISTRIBUIDORA, self.COD_VENDEDOR, self.NOMBRE_VENDEDOR, self.TIPO_DOC_VENDEDOR, self.NUM_DOC_VENDEDOR, self.CANAL_VENDEDOR, self.COD_SUPERVISOR, self.NOMBRE_SUPERVISOR, self.CODIGO_GRUPO))
               conexion.commit() 
               conexion.close()
          

     TESTINSTANCE = Vendedores(None, None, None, None, None, None, None, None, None)
     TESTINSTANCE.Grabar(dfv)
     print('CARGÓ CORRECTAMENTE VENDEDORES A BD')
        
else : 
         print("Faltan columnas , se detiene el proceso.") 
         sys.exit() 



