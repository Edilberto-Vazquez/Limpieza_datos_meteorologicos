import pandas as pd
import glob
import os
from concurrent.futures import ProcessPoolExecutor
import threading
import time


directorio = glob.glob('F:/Archivos/DataSets/Tesis/Datos_Originales/Medidor_Campo_Electrico/*.efm')

print(directorio)

def concat_fecha(directorio):
    directorio = [directorio]
    for archivo in directorio:
        fecha = os.path.basename(archivo).lstrip("INAOE parque-").rstrip(".efm")
        #fecha = fecha[4:]+"-"+fecha[0:2]+"-"+fecha[2:4]
        fecha = (f'{fecha[4:]}-{fecha[0:2]}-{fecha[2:4]}')
        with open(archivo, 'r', encoding='UTF-8') as f_read:
            n_lineas = f_read.readlines()
            with open(f'F:/Archivos/DataSets/Tesis/Datos_Procesados/Medidor_Campo_Electrico/{fecha}.csv', 'a', encoding='UTF-8') as f_write:
                f_write.write('Fecha, Polaridad, NivelCE, FalloRotor\n')
                for linea in n_lineas:
                    linea_n = linea.replace(',0',',correcto').replace(',1',',fallo').replace('+', 'positivo,').replace('-', 'negativo,')
                    f_write.write(f'{fecha} {linea_n}')

if __name__ == "__main__":
    
    t1 = time.time()
    
    with ProcessPoolExecutor(max_workers=4) as executor:
        executor.map(concat_fecha, directorio)
        '''for i, result in enumerate(executor.map(concat_fecha, directorio)):
            print(f'procesado {directorio[i]}')'''
    
    t2 = time.time()
    t_transcurrido = round(t2 - t1, 4)
    print(f'Archivos procesados en {t_transcurrido} segundos.')