import pandas as pd
import numpy as np
import time

seconds_per_week = 604800
"""
NOTA:

Si queremos comprobar que funciona, en el filtrado por fecha
debemos restar la diferencia de tiempo desde los últimos datos
recogidos (aproximadamente 1 año y 1 mes). En un caso real 
esta variable no estaría presente
"""
seconds_difference = 34187400

# Cargamos los datos
dfm = pd.read_csv("movies_procesadas.csv")
dfr = pd.read_csv("ratings_procesados.csv")

# Nos quedamos solo con los datos de la última semana
dfr = dfr[dfr['timestamp'] >= time.time() - seconds_per_week - seconds_difference]

# Eliminamos campos que no nos sirven
dfr = dfr.drop(["timestamp", "userId"], axis = 1)
dfm = dfm.drop("title", axis = 1)

# Separamos los géneros, creando filas nuevas
dfm = dfm.assign(genres=dfm['genres'].str.split('|')).explode('genres')

# Hacemos un join de ambos dataframes por la columna movieId
df = pd.merge(dfm, dfr, on='movieId')

# Descartamos movieId y agrupamos los géneros por las medias de los ratings
df = df.drop("movieId", axis = 1).groupby("genres")["rating"].mean()

# Guardamos el resultado en fichero csv
df.to_csv("pelis_procesadas.csv")
