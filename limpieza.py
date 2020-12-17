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
dfm = dfm[dfm.movieId.apply(lambda x: np.isreal(x))]
dfm = dfm.dropna(subset=['movieId'])

#TODO: poner bien regex
def regex_filter(val):
    regex = "[a-zA-Z\- \t]"
    return val and re.search(regex, val):

dfm = dfm.dropna(subset=['genres'])
dfm = dfm[dfm['genres'].apply(regex_filter)]

# Valores perdidos dfr
dfr = dfr.dropna(subset=['movieId'])
dfr = dfr[dfr.movieId.apply(lambda x: np.isreal(x))]

dfr = dfr.dropna(subset=['rating'])
dfr = dfr[dfr.rating.apply(lambda x: np.isreal(x) and x >= 0 
                           and x <= 5 and int(2*x) == 2*x)]

dfr = dfr[dfr.timestamp.apply(lambda x: np.isreal(x) and x >= 0 
                           and x <= time.time())]

"""
GUARDAR DATOS ACTUALIZADOS
"""

#TODO. Cómo lo gestiona airflow?
