import pandas as pd
import glob
import numpy as np
from concurrent.futures import ProcessPoolExecutor
import time
import os

#funcion que procesa los archivos como texto en lugar de DataFrame
def campo_electrico_promedio(archivos_mc):
    archivos = [archivos_mc]
    df_promedio = np.empty(0)
    for archivo in archivos:
        #se extrae la fecha del archivo
        fecha = os.path.basename(archivo).lstrip("INAOE parque-").rstrip(".efm")
        fecha = fecha[4:]+"-"+fecha[0:2]+"-"+fecha[2:4]
        #se abre el archvivo en modo lectura y se leen las lineas
        with open(archivo, 'r', encoding='UTF-8') as archivo_r:
            lineas=archivo_r.readlines()
            hora = lineas[0][0:8]
            #se crea el archivo procesado
            with open(f'F:/DataSets/Tesis/Datos_Procesados/Medidor_Campo_Electrico/conjunto_procesado_como_texto/INAOE parque-{fecha}.csv', 'a', encoding='UTF-8') as archivo_w:
                #se recorren las lineas del archivo leido
                for linea in lineas:
                    #se concatenan las lineas con horas iguales
                    if linea[0:8] == hora:
                        df_promedio = np.append(df_promedio, float(linea[9:14]))
                        hora = linea[0:8]
                    #se saca el promedio del campo electrico y se guarda y se escribe en el achivo junto con la fecha
                    else:
                        archivo_w.write(f'{fecha} {hora},{round(np.mean(df_promedio), 2)}\n')
                        hora = linea[0:8]
                        df_promedio = np.empty(0)
                        df_promedio = np.append(df_promedio, float(linea[9:14])) if linea[0:8] == hora else print('no')

#funcion para unir los archivos procesados (opcinal)
def archivos_campo_electrico_unidos(directorio):
    df = pd.concat([pd.read_csv(archivo, sep=',', decimal='.', encoding='UTF-8', names=['Fecha', 'NivelCE']) for archivo in directorio], ignore_index = True,)
    df = df.sort_values('Fecha')
    df.drop_duplicates(inplace = True)
    df.to_csv('F:/DataSets/Tesis/Datos_Procesados/Medidor_Campo_Electrico/conjunto_datos_campo_texto.csv', sep=',', decimal='.', index=None, encoding='UTF-8')

if __name__ == "__main__":
    #variable para vizulaizar el tiempo total de ejecucion del programa
    t1 = time.time()
    #se cargan las direcciones de los archivos y se procesan en paralelo llamando a la funcion "campo_electrico_promedio"
    archivos_mc = glob.glob('F:/DataSets/Tesis/Datos_Originales/Medidor_Campo_Electrico/*.efm')
    #en max_workers se pone el numero de procesadors logicos de la CPU
    with ProcessPoolExecutor(max_workers=8) as executor:
        executor.map(campo_electrico_promedio, archivos_mc)
    #se unen todos los archivos en uno solo con la funcion "archivos_campo_electrico_unidos" (opcional)
    archivos_mcp = glob.glob('F:/DataSets/Tesis/Datos_Procesados/Medidor_Campo_Electrico/conjunto_procesado_como_texto/*.csv')
    archivos_campo_electrico_unidos(archivos_mcp)
    #se imprime el tiempo de ejecucion del programa en segundos
    t2 = time.time()
    t_transcurrido = round(t2 - t1, 4)
    print(f'Archivos procesados en {t_transcurrido} segundos.')
    