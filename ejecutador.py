# import subprocess
# import sys
# import os
# import time
# from ejecutadores.funciones import *
# def main():
#     cronometro= time.time()

#     print("_____________________PROCESO ETL_________________")
#     print("IMPORTANTE : Se debe contar con todos los documentos en la carpeta designada y con el nombre establecido.")
#     print("Los documentos solicitados son los siguientes: \n")
#     print("1: Distribuidoras")
#     print("2: Vendedores")
#     print("3: Clientes")
#     print("4: Maestro_General")
#     print("5: Productos")
#     print("6: Equivalencias")
#     print("7: Ventas")


#     opcion = input("Ejecutar proceso de Carga : (Y/N) ")

#     if opcion == "Y":
#         print("Limpiando las tablas.......\n") 
#         limpieza_de_tablas()
#         print("Limpieza completada. \n") 
#         ejecutar_scripts('DISTRIBUIDORAS.py')
#         print("INFORMACIÓN DE DISTRIBUIDORAS PROCESADA. \n")
#         # ejecutar_scripts('VENDEDORES.py')
#         # print("INFORMACIÓN DE VENDEDORES PROCESADA. \n")
#         # ejecutar_scripts('CLIENTES.py')
#         # print("INFORMACIÓN DE CLIENTES PROCESADA. \n")
#         # ejecutar_scripts('MAESTRO_GENERAL.py')
#         # print("INFORMACIÓN DE MAESTRO_GENERAL PROCESADA. \n")
#         # ejecutar_scripts('PRODUCTOS.py')   
#         # print("INFORMACIÓN DE PRODUCTOS PROCESADA. \n")
#         # ejecutar_scripts('EQUIVALENCIAS.py') 
#         # print("INFORMACIÓN DE EQUIVALENCIAS PROCESADA. \n")
#         # ejecutar_scripts('VENTAS.py')  
#         # print("INFORMACIÓN DE VENTAS PROCESADA. \n")
#         print("INFORMACIÓN DE 7 ARCHIVOS PROCESADA. \n")
#         fin_cronometro = time.time()
#         tiempo_ejecucion = (fin_cronometro - cronometro)/60
#         print(f"El proceso completo tomó {tiempo_ejecucion:.2f} minutos.")
#     elif opcion == "N": 
#         print("Cerrando aplicación...\n")
#         sys.exit()
#     else:
#         print("Opción no válida. Intente de nuevo.\n")

# if __name__ == "__main__":
#     main()

# ---------------------------------------------------------------------------------- 

# import subprocess
# import sys
# import os
# import time
# from ejecutadores.funciones import *

# def ejecutar_proceso(etl_ejecutar):
#     if etl_ejecutar.upper() == "Y":
#         cronometro = time.time()

#         print("_____________________PROCESO ETL_________________")
#         print("IMPORTANTE: Se debe contar con todos los documentos en la carpeta designada y con el nombre establecido.")
#         print("Los documentos solicitados son los siguientes: \n")
#         print("1: Distribuidoras")
#         print("2: Vendedores")
#         print("3: Clientes")
#         print("4: Maestro_General")
#         print("5: Productos")
#         print("6: Equivalencias")
#         print("7: Ventas")

#         print("Limpiando las tablas.......\n")
#         limpieza_de_tablas()
#         print("Limpieza completada.\n")

#         ejecutar_scripts('DISTRIBUIDORAS.py')
#         print("INFORMACIÓN DE DISTRIBUIDORAS PROCESADA.\n")
#         # Ejecuta los otros scripts según sea necesario
#         # Ejemplo comentado:
#         # ejecutar_scripts('VENDEDORES.py')
#         # print("INFORMACIÓN DE VENDEDORES PROCESADA.\n")

#         print("INFORMACIÓN DE 7 ARCHIVOS PROCESADA.\n")
#         fin_cronometro = time.time()
#         tiempo_ejecucion = (fin_cronometro - cronometro) / 60
#         print(f"El proceso completo tomó {tiempo_ejecucion:.2f} minutos.")
#     elif etl_ejecutar.upper() == "N":
#         print("Cerrando aplicación...\n")
#     else:
#         print("Opción no válida. Intente de nuevo.\n")

# def main():
#     opcion = input("Ejecutar proceso de Carga : (Y/N) ")
#     ejecutar_proceso(opcion)

# if __name__ == "__main__":
#     main()

import sys
import time
from ejecutadores.funciones import *

def ejecutar_proceso(decision):
    if decision.upper() == "Y":
        cronometro = time.time()

        print("_____________________PROCESO ETL_________________")
        print("IMPORTANTE: Se debe contar con todos los documentos en la carpeta designada y con el nombre establecido.")
        print("Los documentos solicitados son los siguientes: \n")
        print("1: Distribuidoras")
        print("2: Vendedores")
        print("3: Clientes")
        print("4: Maestro_General")
        print("5: Productos")
        print("6: Equivalencias")
        print("7: Ventas")

        print("Limpiando las tablas.......\n")
        limpieza_de_tablas()
        print("Limpieza completada.\n")
        ejecutar_scripts('DISTRIBUIDORAS.py')
        print("INFORMACIÓN DE DISTRIBUIDORAS PROCESADA.\n")
        ejecutar_scripts('VENDEDORES.py')
        print("INFORMACIÓN DE VENDEDORES PROCESADA. \n")
        ejecutar_scripts('CLIENTES.py')
        print("INFORMACIÓN DE CLIENTES PROCESADA. \n")
        ejecutar_scripts('MAESTRO_GENERAL.py')
        print("INFORMACIÓN DE MAESTRO_GENERAL PROCESADA. \n")
        ejecutar_scripts('PRODUCTOS.py')   
        print("INFORMACIÓN DE PRODUCTOS PROCESADA. \n")
        ejecutar_scripts('EQUIVALENCIAS.py') 
        print("INFORMACIÓN DE EQUIVALENCIAS PROCESADA. \n")
        ejecutar_scripts('VENTAS.py')  
        print("INFORMACIÓN DE VENTAS PROCESADA. \n")
        print("INFORMACIÓN DE 7 ARCHIVOS PROCESADA.\n")
        fin_cronometro = time.time()
        tiempo_ejecucion = (fin_cronometro - cronometro) / 60
        print(f"El proceso completo tomó {tiempo_ejecucion:.2f} minutos.")
    elif decision.upper() == "N":
        print("Cerrando aplicación...\n")
    else:
        print("Opción no válida. Intente de nuevo.\n")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        ejecutar_proceso(sys.argv[1])
    else:
        print("No se proporcionó la decisión de ejecución.")
