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
# Manejar Strings como IO buffers/files
import StringIO

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

        dp = DataPresenter(_nombre_csv(url), alias)
        dp.present('file')
        with codecs.open('presentacion-{}.md'.format(alias), 'w',
                         encoding='utf8') as target:
            target.write(dp.presentation.read())
            dp.presentation.seek(0)

# Hashea con MD5 un string arbitrario y devuelve el hash hexadecimal.
def hash_str(string):
    return hashlib.md5(string).hexdigest()

class DataPresenter(object):
    """Objeto a cargo de generar la presentacion del dataset, y escribirla a STDOUT/archivo.

    Attributes:
        df (pandas.DataFrame): DataFrame a analizar, generado a partir del
            contenido de `self.buffer`
        presentation (StringIO): Una vez que se ejecuta el metodo
            `self.present`,  este atributo guarda el contenido del informe.
        hash_id (str): Hash MD5 del buffer que se uso para levantar el dataset.
            Funciona como alias en caso de no tener uno.

    """

    def __init__(self, filepath_or_buffer, alias=None):
        """Inicializa el presentador.

        Al inicializar un objeto DataPresenter, no se genera ningun informe
        todavia. Para ello, se debe invocar el metodo `self.present`, con las
        opciones deseadas, que guardara el informe en el StringIO llamado
        `self.presentation`.

        Args:
            filepath_or_buffer: Puede ser un buffer *abierto*, o el path
            relativo a un CSV para leer a un buffer.
            alias (str): Alias para usar en caso de necesitar nombrar el
            dataset.

        """
        # Inicializo el buffer al cual se va a leer el CSV/dataset
        self.buffer = None
        if type(filepath_or_buffer)==file:
            self.buffer = filepath_or_buffer
        elif type(filepath_or_buffer) in [str,unicode] and os.path.isfile(filepath_or_buffer):
            self.buffer = open(filepath_or_buffer, 'r')
        self.buffer.seek(0)

        # Leo el dataset a un dataframe
        self.df = pd.read_csv(self.buffer, encoding='utf8')
        self.buffer.seek(0)

        # TODO: Es posible que cargar un df desde CSV, trabajarlo y dumpearlo a
        # otro CSV cambie el hash generado?
        # Genero un hash en funcion del contenido del buffer, que se usara como
        # alias de ser necesario
        self.hash_id = hash_str(self.buffer.read())
        self.buffer.seek(0)

        self.alias = alias or hash_id

        # Este objeto guardara el informe generado.
        self.presentation = StringIO.StringIO()

    def present(self, modo = 'stdout'):
        """En funcion de los parametros pasados, rellena `self.presentation`
        con el informe del DataFrame

        Por el momento, el unico parametro aceptado es `modo`, para determinar
        el formato de tabla a utilizar.

        Args:
            modo(str): Puede ser 'stdout' o 'file'. Se utiliza unicamente para
                decidir el formato de la data tabular ('simple' para 'stdout',
                markdown-style para 'file')

        """


        # Vaciar la presentacion, en caso de que y se haya generado una antes
        self.presentation.truncate(0)

        # Funcion para generar representacion tabular de cierto DataFrame en
        # una string.
        def tabular(df):
            formato = 'pipe' if modo=='file' else 'simple'
            return tabulate.tabulate(df, headers='keys', tablefmt=formato)

        ####### COMIENZO INFORME #######

        # Primera seccion: Informacion "global" del dataset:
        # - header,
        # - hash del CSV,
        # - timestamp del informe,
        # - nro de filas y columnas del dataset
        self.presentation.write("# Presentacion de Data Frame {}\n".format(self.alias))

        start_time = time.strftime('%Y/%m/%d@%H:%M:%S', time.localtime())
        self.presentation.write("## Metadata\n- Fecha informe: {}\n- Hash MD5 DataFrame: {}\n".format(start_time, self.hash_id))
        self.presentation.write("## Caracteristicas generales\n")
        self.presentation.write("- Filas: {}\n- Columnas: {}\n".format(len(self.df), len(self.df.columns)))

        # Segunda seccion: Informacion sumaria por columna no-float: 
        # - dtype
        # - cant. valores unicos y
        # - 10 valores mas comunes
        self.presentation.write("## Nombre y tipo de dato por columna:\n")

        for col, dtype in self.df.select_dtypes(exclude=['float64']).dtypes.iteritems():

            # Nombre del campo y `dtype`
            self.presentation.write("### CAMPO `{}` (dtype `{}`)\n".format(col, dtype))

            grupo = self.df.groupby(col)
            self.presentation.write("- Valores unicos: {}\n".format(len(grupo)))
            self.presentation.write("- Diez valores mas comunes:\n")

            f = {col:['count']}
            agg = grupo.aggregate(f)
            top10 = agg[col].sort_values(by='count', ascending=False)[0:9]
            self.presentation.write(tabular(top10))
            self.presentation.write("\n\n")

        ####### FIN INFORME #######

        # Rebobinar presentation
        self.presentation.seek(0)

if __name__ == '__main__':
    descargar_todo(URL_DATASETS_PORTAL)
    presentar_todo(URL_DATASETS_PORTAL)
