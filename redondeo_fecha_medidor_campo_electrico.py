import pandas as pd
import glob
import statistics as st
import numpy as np

directorio = glob.glob('F:/Archivos/DataSets/Tesis/Datos_Procesados/*.csv')
new_archivo = pd.DataFrame()
moda_array = []

for n_archivo in directorio:
    archivo = pd.read_csv(n_archivo, sep=',', decimal='.' ,encoding='UTF-8')
    fecha = archivo.iloc[0, 0]
    for n_linea in range(len(archivo)):
        if archivo.iloc[n_linea, 0] == fecha:
            if n_linea == len(archivo)-1:
                new_archivo = new_archivo.append({'Fecha':fecha, 'Polaridad':archivo.iloc[n_linea, 1], 'NivelCE':st.mode(moda_array), 'FalloRotor':archivo.iloc[n_linea, 3]}, ignore_index=True)
            else:
                moda_array.append(archivo.iloc[n_linea, 2])
        else:
            new_archivo = new_archivo.append({'Fecha':fecha, 'Polaridad':archivo.iloc[n_linea-1, 1], 'NivelCE':st.mode(moda_array), 'FalloRotor':archivo.iloc[n_linea-1, 3]}, ignore_index=True)
            moda_array = []
            fecha = archivo.iloc[n_linea, 0]
new_archivo.to_csv('F:/Archivos/DataSets/Tesis/Datos_Procesados/conjunto_campo_electrico.csv', sep=',', decimal='.', index=None, encoding='UTF-8')


array = [['a',1],['b',2],['c',3]]

array2 = np.array(array)

print(array2[:,1])