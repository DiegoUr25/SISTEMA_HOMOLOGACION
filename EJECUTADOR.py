import subprocess
from FUNCIONES import *
def main():
    print("_____________________PROCESO ETL_________________")
    print("IMPORTANTE : Se debe contar con todos los documentos en la carpeta designada y con el nombre establecido.")
    print("Seleccione el script que desea ejecutar:")
    print("0: Todos los anteriores ")
    print("1: DISTRIBUIDORAS")
    print("2: VENDEDORES")
    print("3: CLIENTES")
    print("4: MAESTRO_GENERAL")
    print("5: PRODUCTOS")
    print("6: EQUIVALENCIAS")
    print("7: VENTAS")


    opcion = input("Ingrese el número del script que desea ejecutar: ")


    if opcion == "0":
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
    elif opcion == "1":
        ejecutar_scripts('DISTRIBUIDORAS.py')
        print("INFORMACIÓN DE DISTRIBUIDORAS PROCESADA. ")
    elif opcion == "2":
        ejecutar_scripts('VENDEDORES.py')
        print("INFORMACIÓN DE VENDEDORES PROCESADA. ")
    elif opcion == "3":
        ejecutar_scripts('CLIENTES.py')
        print("INFORMACIÓN DE CLIENTES PROCESADA. ")
    elif opcion == "4":
        ejecutar_scripts('MAESTRO_GENERAL.py')
        print("INFORMACIÓN DE MAESTRO_GENERAL PROCESADA. ")
    elif opcion == "5":
        ejecutar_scripts('PRODUCTOS.py')   
        print("INFORMACIÓN DE PRODUCTOS PROCESADA. ")
    elif opcion == "6":
        ejecutar_scripts('EQUIVALENCIAS.py') 
        print("INFORMACIÓN DE EQUIVALENCIAS PROCESADA. ")
    elif opcion == "7":
        ejecutar_scripts('VENTAS.py')  
        print("INFORMACIÓN DE VENTAS PROCESADA. ")
    else:
        print("Opción no válida. Intente de nuevo.")

if __name__ == "__main__":
    main()
