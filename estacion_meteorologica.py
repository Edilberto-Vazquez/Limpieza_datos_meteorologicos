import pandas as pd
import glob

directorio = glob.glob('D:/Tesis/Estacion_meteorologica/*.csv')

print(directorio)
    
archivos = pd.concat([pd.read_csv(n_archivo, sep=';', decimal=',' ,encoding='UTF-16LE') for n_archivo in directorio], ignore_index = True,)

archivos = archivos.drop(columns=['Unnamed: 16'])

archivos.to_csv('E:/Datos_tratados/Datos_meteorologicos/conjunto_datos_meteorologicos.csv', sep=',', decimal='.', index=None, encoding='UTF-8')
