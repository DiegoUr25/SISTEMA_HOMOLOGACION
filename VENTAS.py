import pandas as pd 
import sys
import ftfy as f
from FUNCIONES import *

filepath = 'C:/Users/PC - Usuario/Desktop/PRUEBAS_ETL/TABLAS/Ventas.csv' 
# separador = detectar_separador(filepath) 
dfcvent= pd.read_csv(filepath, delimiter = ';', encoding='unicode_escape',dtype='string') 
num_columnas = dfcvent.shape[1]  
print ("Tiene la siguiente cant de columnas : ", num_columnas) 

#dfcvent.info()
###TRANSFORMACION
#VALIDACIÓN DE FORMATO DE COLUMNAS ADMITIDAS PARA EL PROCESO 


print(dfcvent.head(100)) 
print(dfcvent.info())
if num_columnas == 15 :
     dfcvent= corregir_columnas_VENTAS(dfcvent) 
     print(dfcvent.head(98))
     print("Todo ok") 

     class VENTAS(): 
          def __init__(self,COD_DISTRIBUIDORA,COD_VENTA,	NUM_FACTURA,	FECHA,	COD_CLIENTE	,COD_ITEM	,COD_PRODUCTO	,CANTIDAD_UND_MIN,	TIPO_UND_MIN, CANT_UND_MAX, TIPO_UND_MAX,VENTA_SINIGV,VENTA_CONIGV,TIPO_PRODUCTO,COD_VENDEDOR): 
               self.COD_DISTRIBUIDORA = COD_DISTRIBUIDORA 
               self.COD_VENTA = COD_VENTA
               self.NUM_FACTURA = NUM_FACTURA
               self.FECHA = FECHA
               self.COD_CLIENTE = COD_CLIENTE
               self.COD_ITEM = COD_ITEM
               self.COD_PRODUCTO = COD_PRODUCTO
               self.CANTIDAD_UND_MIN = CANTIDAD_UND_MIN
               self.TIPO_UND_MIN = TIPO_UND_MIN 
               self.CANT_UND_MAX = CANT_UND_MAX 
               self.TIPO_UND_MAX = TIPO_UND_MAX
               self.VENTA_SINIGV = VENTA_SINIGV  
               self.VENTA_CONIGV = VENTA_CONIGV
               self.TIPO_PRODUCTO = TIPO_PRODUCTO
               self.COD_VENDEDOR = COD_VENDEDOR
    
          def Grabar(self, dfcvent): 
               conexion=odbc.connect(db_connector())   
               cursor = conexion.cursor() 
               for index, row in dfcvent.iterrows():  
                    self.COD_DISTRIBUIDORA = row['COD_DISTRIBUIDORA'] 
                    self.COD_VENTA = row['COD_VENTA']
                    self.NUM_FACTURA = row['NUM_FACTURA']
                    self.FECHA = row['FECHA']
                    self.COD_CLIENTE = row['COD_CLIENTE']
                    self.COD_ITEM = row['COD_ITEM']
                    self.COD_PRODUCTO = row['COD_PRODUCTO']
                    self.CANTIDAD_UND_MIN = row['CANTIDAD_UND_MIN']
                    self.TIPO_UND_MIN = row['TIPO_UND_MIN'] 
                    self.CANT_UND_MAX = row['CANT_UND_MAX'] 
                    self.TIPO_UND_MAX = row['TIPO_UND_MAX'] 
                    self.VENTA_SINIGV = row['VENTA_SINIGV']   
                    self.VENTA_CONIGV = row['VENTA_CONIGV'] 
                    self.TIPO_PRODUCTO = row['TIPO_PRODUCTO'] 
                    self.COD_VENDEDOR = row['COD_VENDEDOR'] 


                    Insert="""INSERT INTO VENTAS (COD_DISTRIBUIDORA, COD_VENTA, NUM_FACTURA, FECHA, COD_CLIENTE, COD_ITEM, COD_PRODUCTO, CANTIDAD_UND_MIN, TIPO_UND_MIN, CANT_UND_MAX,TIPO_UND_MAX,VENTA_SINIGV,VENTA_CONIGV,TIPO_PRODUCTO,COD_VENDEDOR) 
                              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
         
                    cursor.execute(Insert,(self.COD_DISTRIBUIDORA, self.COD_VENTA, self.NUM_FACTURA, self.FECHA, self.COD_CLIENTE, self.COD_ITEM, self.COD_PRODUCTO, self.CANTIDAD_UND_MIN, self.TIPO_UND_MIN, self.CANT_UND_MAX,self.TIPO_UND_MAX,self.VENTA_SINIGV,self.VENTA_CONIGV,self.TIPO_PRODUCTO,self.COD_VENDEDOR))
               conexion.commit() 
               conexion.close()
          

     TESTINSTANCE = VENTAS(None, None, None, None, None, None, None, None, None, None, None, None, None, None, None)
     TESTINSTANCE.Grabar(dfcvent)
     print('CARGÓ CORRECTAMENTE VENTAS A BD')
        
else : 
         print("Faltan columnas , se detiene el proceso.") 
         sys.exit() 



