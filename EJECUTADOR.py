import subprocess
import sys
from FUNCIONES import *
def main():
    print("_____________________PROCESO ETL_________________")
    print("IMPORTANTE : Se debe contar con todos los documentos en la carpeta designada y con el nombre establecido.")
    print("Los documentos solicitados son los siguientes: ")
    print("")
    print("1: Distribuidoras")
    print("2: Vendedores")
    print("3: Clientes")
    print("4: Maestro_General")
    print("5: Productos")
    print("6: Equivalencias")
    print("7: Ventas")

    opcion = input("Ejecutar proceso de Carga : (Y/N) ")

    if opcion == "Y":
        ejecutar_scripts('DISTRIBUIDORAS.py')
        print("INFORMACIÓN DE DISTRIBUIDORAS PROCESADA. ")
        ejecutar_scripts('VENDEDORES.py')
        print("INFORMACIÓN DE VENDEDORES PROCESADA. ")
        ejecutar_scripts('CLIENTES.py')
        print("INFORMACIÓN DE CLIENTES PROCESADA. ")
        ejecutar_scripts('MAESTRO_GENERAL.py')
        print("INFORMACIÓN DE MAESTRO_GENERAL PROCESADA. ")
        ejecutar_scripts('PRODUCTOS.py')   
        print("INFORMACIÓN DE PRODUCTOS PROCESADA. ")
        ejecutar_scripts('EQUIVALENCIAS.py') 
        print("INFORMACIÓN DE EQUIVALENCIAS PROCESADA. ")
        ejecutar_scripts('VENTAS.py')  
        print("INFORMACIÓN DE VENTAS PROCESADA. ")
        print(" ")
        print("INFORMACIÓN DE 7 ARCHIVOS PROCESADA. ")
    elif opcion == "N": 
        print("Cerrando aplicación...")
        sys.exit()
    else:
        print("Opción no válida. Intente de nuevo.")

if __name__ == "__main__":
    main()
