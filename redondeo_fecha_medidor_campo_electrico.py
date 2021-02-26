import pandas as pd
import glob
import statistics as st

directorio = glob.glob('F:/Archivos/DataSets/Tesis/Datos_Procesados/*.csv')
new_archivo = pd.DataFrame()
moda_array = []

for n_archivo in directorio:
    archivo = pd.read_csv(n_archivo, sep=',', decimal='.' ,encoding='UTF-8')
    negativo = archivo[archivo.Polaridad == 'negativo']
    fecha = negativo.iloc[0, 0]
    for n_linea in range(len(negativo)):
        if negativo.iloc[n_linea, 0] == fecha:
            if n_linea == len(negativo)-1:
                new_archivo = new_archivo.append({'Fecha':fecha, 'Polaridad':negativo.iloc[n_linea, 1], 'NivelCE':round(st.mean(moda_array),2), 'FalloRotor':negativo.iloc[n_linea, 3]}, ignore_index=True)
            else:
                moda_array.append(negativo.iloc[n_linea, 2])
        else:
            print(st.mode(moda_array))
            new_archivo = new_archivo.append({'Fecha':fecha, 'Polaridad':negativo.iloc[n_linea-1, 1], 'NivelCE':round(st.mean(moda_array),2), 'FalloRotor':negativo.iloc[n_linea-1, 3]}, ignore_index=True)
            moda_array = [negativo.iloc[n_linea, 2]]
            fecha = negativo.iloc[n_linea, 0]
new_archivo.to_csv('F:/Archivos/DataSets/Tesis/Datos_Procesados/conjunto_campo_electrico.csv', sep=',', decimal='.', index=None, encoding='UTF-8')