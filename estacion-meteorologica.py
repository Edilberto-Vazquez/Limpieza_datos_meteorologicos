import pandas as pd
import glob

#se carga la direccion de los archivos de la estacion meteorologica
archivos_em = glob.glob('F:/DataSets/Conjuntos-originales/Conjutno-original-estacion-meteorologica/*.csv')
#se concatenan los archivos en uno solo    
df = pd.concat([pd.read_csv(n_archivo, sep=';', decimal=',' ,encoding='UTF-16LE') for n_archivo in archivos_em], ignore_index = True,)
#se eliminan las columnas que no se utilizaran
df = df.drop(columns=['Tempin (°C)', 'Dewin (°C)', 'Heatin (°C)', 'Humin (%)', 'Wspdhi (km/h)', 'Wdiravg (°)', 'Rainrate (mm/h)', 'Unnamed: 16'])
#se cambian los nombres de las columnas
df.set_axis(['Fecha' ,'Temp', 'Chill', 'Dew', 'Heat', 'Hum', 'Wspdavg', 'Bar', 'Rain'], axis='columns', inplace=True)
#se eliminan los valores nulos
df.dropna()
#se guardan los archivos
df.to_csv('F:/DataSets/Conjuntos-procesados/Conjunto-procesado-estacion-meteorologica/datos-meteorologicos-2019-conjunto-procesado.csv', sep=',', decimal='.', index=None, encoding='UTF-8')