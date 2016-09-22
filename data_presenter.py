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

# Seteo de locale para reconocer "," como separador decimal
#import locale
#locale.setlocale(locale.LC_ALL, locale='es_AR.utf8')

# Diccionario creado a mano con alias y URL de todo CSV del portal
URL_DATASETS_PORTAL = {
    'estructura-organica-pen': 'http://datos.gob.ar/dataset/ad5b0e15-a9ed-40d5-9827-33a0ece12433/resource/b705d8c1-650f-43cc-bde6-68850fcecd21/download/estructura-organica.csv',
                       'audiencias': 'http://datos.gob.ar/dataset/2889b09e-31ca-4f90-912e-2a50db874e33/resource/dcc74a80-55fa-4e9c-b025-e0d321aa36ee/download/audiencias.csv',
                       'salarios-pen': 'http://datos.gob.ar/dataset/431381cc-3c5b-49ba-bf77-47bf658cd640/resource/d3fe3a9a-551b-407d-ba19-bfbad00f86e5/download/salarios-2016.csv',
                       'presupuesto': 'http://datos.gob.ar/dataset/89f1a2dd-ad79-4211-87b4-44661d81ac0d/resource/84e23782-7d52-4724-a4ba-2f9621fa5f4e/download/presupuesto-2016.csv',
                       'declaraciones-juradas': 'http://datos.gob.ar/dataset/5dde9735-6a0a-4f85-8afd-afc6854c3c2c/resource/f5101909-31be-45f5-aba8-c2b2459d29d5/download/declaraciones-juradas-2015.csv',
                       'acceso-informacion-publica': 'http://datos.gob.ar/dataset/8bc053c8-efc2-485d-97d3-915c476d2741/resource/63952097-cdba-4fdd-be84-65fb400bdb1a/download/acceso-informacion-publica.csv',
                       'pauta-oficial': 'http://datos.gob.ar/dataset/122808ec-dcd1-4a9b-aafe-8fa80ac2a2f4/resource/0c3cca0a-ccfa-4520-a614-dbdd58d74d79/download/pauta-oficial-primer-semestre-2016.csv',
                       'contratos': 'http://datos.gob.ar/dataset/becaceb2-dbd0-4879-93bd-5f02bd3b8ca2/resource/bf2f67f4-9ab3-479b-a881-56b43565125e/download/contratos-2015.csv',
                       'contrataciones-convocatorias': 'http://datos.gob.ar/dataset/069b5833-e57d-4d7a-859b-67a80cfdff20/resource/fa3603b3-0af7-43cc-9da9-90a512217d8a/download/convocatorias-2015.csv',
                       'contrataciones-adjudicaciones': 'http://datos.gob.ar/dataset/069b5833-e57d-4d7a-859b-67a80cfdff20/resource/41fcfdb2-fdb3-4855-89b2-09d9f7c6bbc8/download/adjudicaciones-2015.csv',
                       'contrataciones-proveedores-sipro': 'http://datos.gob.ar/dataset/069b5833-e57d-4d7a-859b-67a80cfdff20/resource/c19a2467-5232-41ae-bb0c-75dcd71e7c5f/download/proveedores-sipro.csv',
                       'contrataciones-items-sibys': 'http://datos.gob.ar/dataset/069b5833-e57d-4d7a-859b-67a80cfdff20/resource/8d0dafbd-bd9c-48b6-81c3-05f492647974/download/items-sibys.csv'
                       }

# Devuelve el nombre del archivo a descargar, asumiendo que es la string entre la ultima "/" y el final de la URL
def _nombre_csv(url):
    return url.split("/")[-1]

def save_to_file(url):
    """Dada la URL de un archivo, descargarlo y guardarlo en un directorio harcodeado (data/crudo/)

    Args:
        url (str): URL del archivo a descargar.
        
    #Returns:
    
    #Side Effects:
        Le pega al marido, esconde alcohol en las lamparas
        
    Raises:
        UnicodeDecodeError: Cuando se le canta
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

# Habria que agregarle un argumento de verbosidad a esta funcion
def descargar_todo(url_dict):
    # En docstring, agregar ejemplo de url_dict valido. En esta y cualquier otra variable
    # donde el tipo no alcance para entender cuales son los valores validos.
    for alias, url in url_dict.iteritems():
        print("Descargando dataset \"{}\"... ".format(alias), end="")
        sys.stdout.flush()
        save_to_file(url)

def presentar_todo(url_dict):
    for alias, url in URL_DATASETS_PORTAL.iteritems():
        print("Procesando dataset {}".format(alias))

        df = pd.read_csv(_nombre_csv(url), encoding='utf8')
        dp = DataPresenter(df, alias)
        dp.present('file') 
    
    print("Procesamiento terminado!")

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
    #presentar_todo(URL_DATASETS_PORTAL)
