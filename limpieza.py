import pandas as pd
import numpy as np
import time
import re
from itertools import chain

"""
CARGA DE DATOS

Si los datos los obtuviésemos de un recurso remoto, podríamos
leerlos con wget con las siguientes órdenes:

import wget
url = 'https://path/to/file'
filename = wget.download(url)

Para simplificar supondremos que ya los tenemos en local.
"""

# Carga de datos
dfm = pd.read_csv("movies.csv")
dfr = pd.read_csv("ratings.csv")

"""
TRATAMIENTO VALORES PERDIDOS
"""

# Eliminar duplicados
dfm = dfm.drop_duplicates()
dfr = dfr.drop_duplicates()

# Valores perdidos dfm
dfm = dfm[np.isreal(dfm['movieId'])]
dfm = dfm.dropna(subset=['movieId'])

def regex_filter(val):
    regex = "[a-zA-Z \-]+(\|[a-zA-Z \-]+)*$"
    if val and re.fullmatch(regex, val):
        return True
    return False

dfm = dfm[dfm['genres'].apply(regex_filter)]

# Valores perdidos dfr
dfr = dfr.dropna(subset=['movieId'])
dfr = dfr[np.isreal(dfr['movieId'])]

dfr = dfr.dropna(subset=['rating'])
dfr = dfr[(np.isreal(dfr['rating'])) & 
          (dfr['rating'].isin(np.arange(0, 5.5, 0.5)))]

dfr = dfr.dropna(subset=['timestamp'])
dfr = dfr[(np.isreal(dfr['timestamp'])) & 
          (dfr['timestamp'] >= 0) &
          (dfr['timestamp'] <= time.time())]

"""
GUARDAR DATOS ACTUALIZADOS
"""

dfm.to_csv("movies_procesadas.csv", index = False)
dfr.to_csv("ratings_procesados.csv", index = False)