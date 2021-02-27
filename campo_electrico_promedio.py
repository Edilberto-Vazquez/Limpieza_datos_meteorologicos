import pandas as pd
import glob
from multiprocessing import Pool
import time
import statistics as st
import numpy as np

#directorio = glob.glob('F:/Archivos/DataSets/Tesis/Datos_Procesados/Medidor_Campo_Electrico/negativo/*.csv')


def promedio_campo(df):
    df_promedio = pd.DataFrame()
    fecha = df.iloc[0, 0]
    media_array = []
    for linea in range(len(df)):
        if df.iloc[linea, 0] == fecha:
            if linea == len(df)-1:
                df_promedio = df_promedio.append({'Fecha':fecha, 'Polaridad':df.iloc[linea, 1], 'NivelCE':round(st.mean(media_array),2), 'FalloRotor':df.iloc[linea, 3]}, ignore_index=True)
            else:
                media_array.append(df.iloc[linea, 2])
        else:
            df_promedio = df_promedio.append({'Fecha':fecha, 'Polaridad':df.iloc[linea-1, 1], 'NivelCE':round(st.mean(media_array),2), 'FalloRotor':df.iloc[linea-1, 3]}, ignore_index=True)
            media_array = [df.iloc[linea, 2]]
            fecha = df.iloc[linea, 0]
    return df_promedio


if __name__ == "__main__":
    
    t1 = time.time()
    directorio = glob.glob('F:/Archivos/DataSets/Tesis/Datos_Procesados/Medidor_campo_Electrico/negativo/*.csv')
    df = pd.DataFrame()
    df_promedio = pd.DataFrame()
    for archivo in directorio:
        df = pd.read_csv(archivo, sep=',', decimal='.' ,encoding='UTF-8')
        cores = 1 if len(df) < 8 else 4
        df_split = np.array_split(df, cores)
        with Pool(4) as executor:
            data = executor.map(promedio_campo, df_split)
            for a in data:
                df_promedio = pd.concat([a, df_promedio])
            #♦a = np.array(data)
    
    df_promedio.to_csv('F:/Archivos/DataSets/Tesis/Datos_Procesados/Medidor_Campo_Electrico/negativo/conjunto_campo_electrico_negativo.csv', sep=',', decimal='.', index=None, encoding='UTF-8')
            
    t2 = time.time()
    t_transcurrido = round(t2 - t1, 4)
    print(f'Archivos procesados en {t_transcurrido} segundos.')