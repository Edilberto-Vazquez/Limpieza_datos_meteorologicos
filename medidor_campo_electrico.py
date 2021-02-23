import pandas as pd
import glob
import os
from concurrent.futures import ThreadPoolExecutor
import threading
import time


directorio = glob.glob('C:/Users/hxh_1/Desktop/elementos_prueba/*.efm')

#print(directorio)

def concat_fecha(directorio):
    directorio = [directorio]
    print(directorio)
    for archivo in directorio:
        fecha = os.path.basename(archivo).lstrip("INAOE parque-").rstrip(".efm")
        fecha = fecha[4:]+"-"+fecha[0:2]+"-"+fecha[2:4]
        with open(archivo, 'r', encoding='UTF-8') as f_read:
            n_lineas = f_read.readlines()
            with open('C:/Users/hxh_1/Desktop/elementos_prueba/conjunto_campo_electrico.csv', 'a', encoding='UTF-8') as f_write:
                f_write.write('Fecha, Polaridad, NivelCE, FalloRotor')
                for linea in range(len(n_lineas)):
                    linea_n = n_lineas[linea].replace(',0',',correcto').replace(',1',',fallo').replace('+', 'positivo,').replace('-', 'negativo,')
                    f_write.write(fecha+' '+linea_n)

if __name__ == "__main__":
    
    t1 = time.time()
    
    with ThreadPoolExecutor() as executor:
        for i, result in enumerate(executor.map(concat_fecha, directorio)):
            print(f'procesado {directorio[i]}')
    
    t2 = time.time()
    elapsed = round(t2 - t1, 4)
    print(f"Processed data in {elapsed} seconds.")