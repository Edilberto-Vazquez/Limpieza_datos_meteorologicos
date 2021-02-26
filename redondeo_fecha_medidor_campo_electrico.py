import pandas as pd
import glob
import statistics as st

def media_polaridad(archivo):
    new_archivo = pd.DataFrame()
    media_array = []
    fecha = archivo.iloc[0, 0]
    for n_linea in range(len(archivo)):
        if archivo.iloc[n_linea, 0] == fecha:
            if n_linea == len(archivo):
                new_archivo = new_archivo.append({'Fecha':fecha, 'Polaridad':archivo.iloc[n_linea, 1], 'NivelCE':round(st.mean(media_array),2), 'FalloRotor':archivo.iloc[n_linea, 3]}, ignore_index=True)
            else:
                media_array.append(archivo.iloc[n_linea, 2])
        else:
            new_archivo = new_archivo.append({'Fecha':fecha, 'Polaridad':archivo.iloc[n_linea-1, 1], 'NivelCE':round(st.mean(media_array),2), 'FalloRotor':archivo.iloc[n_linea-1, 3]}, ignore_index=True)
            media_array = [archivo.iloc[n_linea, 2]]
            fecha = archivo.iloc[n_linea, 0]
    return new_archivo

directorio = glob.glob('F:/Archivos/DataSets/Tesis/Datos_Procesados/Medidor_Campo_Electrico/*.csv')
new_archivo2 = pd.DataFrame()
for n_archivo in directorio:
    archivo = pd.read_csv(n_archivo, sep=',', decimal='.' ,encoding='UTF-8')
    df_negativo, df_positivo = archivo[archivo.Polaridad == 'negativo'], archivo[archivo.Polaridad == 'positivo']
    df_negativo = print('data frame vacio') if df_negativo.empty is True else media_polaridad(df_negativo)
    df_positivo = print('data frame vacio') if df_positivo.empty is True else media_polaridad(df_positivo)
    new_archivo = pd.concat([df_negativo,df_positivo], ignore_index = True,)
    new_archivo2 = pd.concat([new_archivo, new_archivo2], ignore_index = True,)
    print(n_archivo)
new_archivo2.to_csv('F:/Archivos/DataSets/Tesis/Datos_Procesados/Medidor_Campo_Electrico/conjunto_campo_electrico.csv', sep=',', decimal='.', index=None, encoding='UTF-8')