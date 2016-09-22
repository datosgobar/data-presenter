# -*- coding: 'utf8' -*-
# unicode_literals asume que toda string del programa esta en unicode
from __future__ import unicode_literals
# fuerza a print() a tomar argumentos entre parentesis. que mas hace?
from __future__ import print_function

# Librerias de analisis
import pandas as pd
# Descargar archivos mediante requests HTTP
import requests
# Escribir archivos en formto Unicode
import codecs
# Varios metodos para lidiar con archivos
import os.path
# Hashear strings arbitrarias de texto para
# - Generar alias de los datasets que no tengan uno, e
# - Identificar versiones de cada dataset
import hashlib
# Guardar timestamp de la generacion del informe
import time
# Tablas con bellos formatos a partir de estructuras de datos tabulares.
import tabulate
# Imprimir lo que este en cola en STDOUT cuando uno lo pida
import sys
# Leer variables de archivos de configuracion
import yaml

# Diccionario creado a mano con alias y URL de todo CSV del portal
URLS_FILE = 'urls-datasets-portal.yaml'
with open(URLS_FILE) as f:
    URL_DATASETS_PORTAL = yaml.load(f)

# Devuelve el nombre del archivo a descargar, asumiendo que es la string entre la ultima "/" y el final de la URL
def _nombre_csv(url):
    return url.split("/")[-1]

def save_to_file(url):
    """Dada la URL de un archivo, descargarlo y guardarlo en el directorio actual.

    Args:
        url (str): URL del archivo a descargar.
        
    Side Effects:
        Outputea mensajes de status con `print()`.
    """
    
    # Crear el archivo objetivo con codificacion UTF8 y escribir el contenido del request
    path = _nombre_csv(url)

    if os.path.isfile(path):
        print("IGNORANDO - el archivo {} ya existe.".format(path))
    
    else:
        # Obtener el CSV
        req = requests.get(url)
        req.encoding = 'utf8'

        with codecs.open(path, 'w', encoding='utf8') as target:
            # Escribir al archivo objetivo
            target.write(req.text)

        print("OK - el archivo {} se guardo exitosamente.".format(path))

def descargar_todo(url_dict):
    """Dado un diccionario cuyos valores sean URLs de archivos, guardarlos en el directorio actual.

    Las claves del diccionario se consideran "alias" para los archivos a descargar.
    El nombre del archivo al que apunta la URL, se asume igual al string de texto entre la ultima barra "/" y el final de la URL. Por ejemplo, "http://datos.gob.ar/tremendo-archivo.csv" se guardara a "tremendo-archivo.csv".

    Un ejemplo de `url_dict` valido podria ser el siguiente:
	{ 'autos': 'https://www.cars.com/car-models.csv',
        'bicis': 'http://www.americaenbici.com/dosruedas.txt'}

    Args:
        url_dict (str): URL del archivo a descargar.
        
    Side Effects:
        Outputea mensajes de status con `print()`.
    """
    for alias, url in url_dict.iteritems():
        # sys.stdout.flush() fuerza la impresion de lo que se haya requerido hasta el momento
        print("Descargando dataset \"{}\"... ".format(alias), end=""); sys.stdout.flush()
        save_to_file(url)

def presentar_todo(url_dict):
    """Dado un diccionario cuyos valores sean URLs de archivos, generar la presentacion de cada uno y guardarla en el directorio actual.

    Las claves del diccionario se consideran "alias" para los archivos a presentar. El archivo generado se llamara `presentacion-<alias>.md`

    El archivo al que apunta la URL se asume descargado al directorio actual con `descargar_todo()` o a mano.

    Un ejemplo de `url_dict` valido podria ser el siguiente:
	{ 'autos': 'https://www.cars.com/car-models.csv',
        'bicis': 'http://www.americaenbici.com/dosruedas.txt'}

    Args:
        url_dict (str): URL del archivo a descargar.
        
    Side Effects:
        Outputea mensajes de status con `print()`.
    """
    for alias, url in URL_DATASETS_PORTAL.iteritems():
        print("Procesando dataset {}".format(alias))

        df = pd.read_csv(_nombre_csv(url), encoding='utf8')
        dp = DataPresenter(df, alias)
        dp.present('file') 
    
    print("Procesamiento terminado!")

# Hashea con MD5 un string arbitrario y devuelve el hash hexadecimal.
def hash_str(string):
    return hashlib.md5(string).hexdigest()

class DataPresenter(object):
    
    def __init__(self, df, alias=None):
        self.df = df
        # TODO: Es posible que cargar un df desde CSV, trabajarlo y dumpearlo a otro CSV cambie el hash generado?
        self.hash_id = hash_str(df.to_csv(encoding='utf8'))
        self.alias = alias or hash_id
        self.target = None
        
    # TODO: Separar generacion del informe de la presentacion del resultado (en STDOUT o file)
    # Sugerencia: Libreria `stringIO`
    def present(self, modo = 'stdout', filename = None):
        
        start_time = time.strftime('%Y/%m/%d@%H:%M:%S', time.localtime())
        
        if modo == 'file':
            path_to_file = "presentacion-{}.md".format(filename or self.alias)
            self.target = open(path_to_file, 'w')

        # Funcion para imprimir output a consola o archivo
        def reportar(string):
            if self.target:
                self.target.write("{}\n".format(string).encode('utf8'))
            else:
                print(string)
                
        def tabular(df):
            formato = 'pipe' if self.target else 'simple'
            return tabulate.tabulate(df, headers='keys', tablefmt=formato)
        
        reportar("# Presentacion de Data Frame {}".format(self.alias))
        reportar("## Metadata\n- Fecha informe: {}\n- Hash MD5 DataFrame: {}\n".format(start_time, self.hash_id))
        reportar("## Caracteristicas generales")
        reportar("- Filas: {}\n- Columnas: {}\n".format(len(self.df), len(self.df.columns)))
        reportar("## Nombre y tipo de dato por columna:")
        
        for col, dtype in self.df.select_dtypes(exclude=['float64']).dtypes.iteritems():

            reportar("### CAMPO `{}` (dtype `{}`)\n".format(col, dtype))
            
            # TODO: Agregar comments inline explicando que parte del output genera cierto fragmento de codigo
            grupo = self.df.groupby(col)       
            reportar("- Valores unicos: {}\n".format(len(grupo)))
            reportar("- Diez valores mas comunes:\n")
            
            f = {col:['count']}
            agg = grupo.aggregate(f)
            top10 = agg[col].sort_values(by='count', ascending=False)[0:9]
            reportar(tabular(top10))
            reportar("\n")
            
        if self.target:
            self.target.close()

if __name__ == '__main__':
    descargar_todo(URL_DATASETS_PORTAL)
    presentar_todo(URL_DATASETS_PORTAL)
