import pandas as pd
import glob
import os

directorio = glob.glob('C:/Users/hxh_1/Desktop/elementos_prueba/*.efm')

print(directorio)

for archivo in directorio:
    fecha = os.path.basename(archivo).lstrip("INAOE parque-").rstrip(".efm")
    fecha = fecha[4:]+"-"+fecha[0:2]+"-"+fecha[2:4]
    with open(archivo, 'r', encoding='UTF-8') as f_read:
        n_lineas = f_read.readlines()
        with open('C:/Users/hxh_1/Desktop/elementos_prueba/conjunto_campo_electrico.csv', 'a', encoding='UTF-8') as f_write:
            for linea in range(len(n_lineas)):
                linea_n = n_lineas[linea].replace(',0',',correcto').replace(',1',',fallo').replace('+', 'positivo,').replace('-', 'negativo,')
                f_write.write(fecha+' '+linea_n)

#with open('C:/Users/hxh_1/Desktop/elementos_prueba/conjunto_campo_electrico', 'w', encoding='UTF-8') as w:

#archivos = pd.concat([pd.read_csv(n_archivo, sep=',', decimal='.', encoding='utf-8',) for n_archivo in directorio], ignore_index=True)

