import pandas as pd 
import sys
import ftfy as f
from datetime import *
from funciones import *
import os

# filepath_output =r'C:/Users/PC - Usuario/Desktop/TESIS/ARCHIVOS/ARCHIVOS_PROCESADOS'
# filepath_input = 'C:/Users/PC - Usuario/Desktop/PRUEBAS_ETL/TABLAS/Maestro_Distribuidoras.csv' 

# Camino al directorio raíz desde la ubicación del script actual
root_directory = os.path.dirname(os.path.dirname(__file__))

# Construye las rutas hacia las carpetas input y output en el directorio raíz
filepath_input = os.path.join(root_directory, 'input/Maestro_Distribuidoras.csv')
filepath_output = os.path.join(root_directory, 'output')

# print (filepath_input, filepath_output)

###TRANSFORMACION SSSdfddd
    #VALIDACIÓN DE FORMATO DE COLUMNAS ADMITIDAS PARA EL PROCESO 
    #TODO:TABLA DE TABLAS REGISTRO DE FECHA DE CARGA COMO HISTÓRICO 
    #TODO:WHAT IF RECIBES UNA COLUMNA DE NÚMEROS TE PONEN UN TEXTO CORREGIR EN FUNCIONES DE LIMPIEZA
    #TODO:USAR EL INDEX DEL BUCLE PARA QUE TE DIGA DÓNDE SUCEDIÓ EL ERROR , INCLUIR UN ARCHIVO QUE TE INDIQUE EN QUÉ TABLA Y DÓNDE ESTÁ EL ERROR, FECHA Y HORA DE INICIO Y FIN

class DISTRIBUIDORAS(): 
            def __init__(self,COD_DISTRIBUIDORA,	RUC_DISTRIBUIDORA,	RAZ_SOCIAL_DISTRIBUIDORA,	NOMBRE_DISTRIBUIDORA,	REGION_DISTRIBUIDORA,	DEPARTAMENTO_DISTRIBUIDORA,	DISTRITO_DISTRIBUIDORA,	ESTADO_DISTRIBUIDORA): 
                self.COD_DISTRIBUIDORA = COD_DISTRIBUIDORA 
                self.RUC_DISTRIBUIDORA = RUC_DISTRIBUIDORA
                self.RAZ_SOCIAL_DISTRIBUIDORA = RAZ_SOCIAL_DISTRIBUIDORA
                self.NOMBRE_DISTRIBUIDORA = NOMBRE_DISTRIBUIDORA
                self.REGION_DISTRIBUIDORA = REGION_DISTRIBUIDORA
                self.DEPARTAMENTO_DISTRIBUIDORA = DEPARTAMENTO_DISTRIBUIDORA
                self.DISTRITO_DISTRIBUIDORA = DISTRITO_DISTRIBUIDORA
                self.ESTADO_DISTRIBUIDORA = ESTADO_DISTRIBUIDORA
                
        
            def Grabar(self, dfd):  
                conexion=odbc.connect(db_connector())   
                cursor = conexion.cursor()

                try:
                    cursor.execute("BEGIN TRANSACTION")      
                    for index, row in dfd.iterrows():  
                        self.COD_DISTRIBUIDORA = row['COD_DISTRIBUIDORA'] 
                        self.RUC_DISTRIBUIDORA = row['RUC_DISTRIBUIDORA']
                        self.RAZ_SOCIAL_DISTRIBUIDORA = row['RAZ_SOCIAL_DISTRIBUIDORA']
                        self.NOMBRE_DISTRIBUIDORA = row['NOMBRE_DISTRIBUIDORA']
                        self.REGION_DISTRIBUIDORA = row['REGION_DISTRIBUIDORA']
                        self.DEPARTAMENTO_DISTRIBUIDORA = row['DEPARTAMENTO_DISTRIBUIDORA']
                        self.DISTRITO_DISTRIBUIDORA = row['DISTRITO_DISTRIBUIDORA']
                        self.ESTADO_DISTRIBUIDORA = row['ESTADO_DISTRIBUIDORA']
                                    


                        Insert="""INSERT INTO DISTRIBUIDORAS (COD_DISTRIBUIDORA,RUC_DISTRIBUIDORA,RAZ_SOCIAL_DISTRIBUIDORA,NOMBRE_DISTRIBUIDORA,REGION_DISTRIBUIDORA,DEPARTAMENTO_DISTRIBUIDORA,DISTRITO_DISTRIBUIDORA,ESTADO_DISTRIBUIDORA) 
                                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)"""
                        
                        cursor.execute(Insert,(
                            self.COD_DISTRIBUIDORA,
                            self.RUC_DISTRIBUIDORA,
                            self.RAZ_SOCIAL_DISTRIBUIDORA,
                            self.NOMBRE_DISTRIBUIDORA,
                            self.REGION_DISTRIBUIDORA,
                            self.DEPARTAMENTO_DISTRIBUIDORA,
                            self.DISTRITO_DISTRIBUIDORA,
                            self.ESTADO_DISTRIBUIDORA
                            ))
                            
                        conexion.commit() 
                        
                except Exception as e: 
                        conexion.rollback()
                        print(f"Error al grabar Distribuidoras: {e}")
                finally: 
                        print('CARGÓ CORRECTAMENTE DISTRIBUIDORAS A BD')
                        conexion.close()

if filepath_input.endswith('.csv'): 
    separador = detectar_separador(filepath_input) 
    fecha_actual = datetime.now().strftime("%Y%m%d_%H%M%S")
    dfd= pd.read_csv(filepath_input, delimiter = separador, encoding='unicode_escape',dtype='string') 
    num_columnas = dfd.shape[1]  
    print ("Tiene la siguiente cant de columnas : ", num_columnas) 
    
    print(dfd.head(100)) 
    print(dfd.info())
    
    if num_columnas == 8 :
        dfd= corregir_columnas_DISTRIBUIDORAS(dfd) 
        #TODO: AGREGAR PROCESAMIENTO DE CSV CON TIMESTAMP Y CREAR ARCHIVO CSV OUTPUT
        print(dfd.head(98))
        print("Todo ok") 

        
        TESTINSTANCE = DISTRIBUIDORAS(None, None, None, None, None, None, None, None)  
        TESTINSTANCE.Grabar(dfd) 
        nombre_tabla="DISTRIBUIDORAS"
        nombre_output = f"{filepath_output}\\{nombre_tabla}_{fecha_actual}.csv" 

        dfd.to_csv(nombre_output, index=False)
        print(f"Archivo procesado guardado como {nombre_output}")
    else : 
            print("Faltan columnas , se detiene el proceso.") 
            sys.exit() 

elif filepath_input.endswith('.txt'):

    separador = detectar_separador(filepath_input) 
    fecha_actual = datetime.now().strftime("%Y%m%d_%H%M%S")

    dfd= pd.read_csv(filepath_input, delimiter = separador, encoding='unicode_escape',dtype='string') 
    num_columnas = dfd.shape[1]  
    print ("Tiene la siguiente cant de columnas : ", num_columnas) 

    print(dfd.head(100)) 
    print(dfd.info())
    if num_columnas == 8 :
        dfd= corregir_columnas_DISTRIBUIDORAS(dfd) 
        #TODO: AGREGAR PROCESAMIENTO DE CSV CON TIMESTAMP Y CREAR ARCHIVO CSV OUTPUT
        print(dfd.head(98))
        print("Todo ok") 
        TESTINSTANCE = DISTRIBUIDORAS(None, None, None, None, None, None, None, None)  
        TESTINSTANCE.Grabar(dfd) 
        nombre_tabla="DISTRIBUIDORAS"
        nombre_output = f"{filepath_output}\\{nombre_tabla}_{fecha_actual}.txt" 
        dfd.to_csv(nombre_output,sep=';' ,index=False)
        print(f"Archivo procesado guardado como {nombre_output}")     
    else : 
            print("Faltan columnas , se detiene el proceso.") 
            sys.exit() 

elif filepath_input.endswith('.xslx'): 
    #separador = detectar_separador(filepath_input) 
    fecha_actual = datetime.now().strftime("%Y%m%d_%H%M%S")
    dfd= pd.read_excel(filepath_input, sheet_name=0, engine='openpyxl',dtype='string') 
    num_columnas = dfd.shape[1]  
    print ("Tiene la siguiente cant de columnas : ", num_columnas) 

    print(dfd.head(100)) 
    print(dfd.info())
    if num_columnas == 8 :
        dfd= corregir_columnas_DISTRIBUIDORAS(dfd) 
        #TODO: AGREGAR PROCESAMIENTO DE CSV CON TIMESTAMP Y CREAR ARCHIVO CSV OUTPUT
        print(dfd.head(98))
        print("Todo ok") 
        TESTINSTANCE = DISTRIBUIDORAS(None, None, None, None, None, None, None, None)  
        TESTINSTANCE.Grabar(dfd) 
        nombre_tabla="DISTRIBUIDORAS"
        nombre_output = f"{filepath_output}\\{nombre_tabla}_{fecha_actual}.csv" 
        dfd.to_csv(nombre_output, index=False)
        print(f"Archivo procesado guardado como {nombre_output}")     
    else : 
            print("Faltan columnas , se detiene el proceso.") 
            sys.exit()  

else : 
      print("Formato de archivo no permitido. /n")
      print("Los formatos permitidos son .txt, .csv y .xslx")
      sys.exit()
