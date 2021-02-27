import pandas as pd
import glob
from concurrent.futures import ProcessPoolExecutor
import time
import os

def  polaridad_campo(directorio):
    directorio = [directorio]
    #df_positivo = pd.DataFrame()
    df_negativo = pd.DataFrame()
    
    for archivo in directorio:
        nombre = os.path.basename(archivo)
        df = pd.read_csv(archivo, sep=',', decimal='.' ,encoding='UTF-8')
    
        '''if df[df.Polaridad == 'positivo'].empty:
            print('vacio')
        else:
            df_positivo = pd.concat([df_positivo, df[df.Polaridad == 'positivo']])
            df_positivo.to_csv(f'F:/Archivos/DataSets/Tesis/Datos_Procesados/Medidor_Campo_Electrico/positivo/{nombre}', sep=',', decimal='.', index=None, encoding='UTF-8')'''
        
        if df[df.Polaridad == 'negativo'].empty:
            print('vacio')
        else:
            df_negativo = pd.concat([df_negativo, df[df.Polaridad == 'negativo']])
            df_negativo.to_csv(f'F:/Archivos/DataSets/Tesis/Datos_Procesados/Medidor_Campo_Electrico/negativo/{nombre}', sep=',', decimal='.', index=None, encoding='UTF-8')


if __name__ == "__main__":
    
    t1 = time.time()
    directorio = glob.glob('F:/Archivos/DataSets/Tesis/Datos_Procesados/Medidor_Campo_Electrico/*.csv')
    df = pd.DataFrame()
    df_1 = pd.DataFrame()
    
    with ProcessPoolExecutor(max_workers=4) as executor:
        executor.map(polaridad_campo, directorio)
            
    t2 = time.time()
    t_transcurrido = round(t2 - t1, 4)
    print(f'Archivos procesados en {t_transcurrido} segundos.')