# data-presenter
Genera estadisticas basicas sobre un dataset en formato CSV. Incluye algunas funciones auxiliares útiles, para descargar CSVs a partir de URLs.

## Índice 
* [Instalación](#instalacion) 
* [Dependencias](#dependencias) 
* [Uso de `data-presenter`](#uso-de-data-presenter) 
* [Créditos](#créditos) 
* [Contacto](#contacto) 

## Instalacion

Clonar el repositorio, y desde la raíz:
```python
pip install -r requirements.txt
```
### Dependencias 

Este proyecto utiliza Python 2.7. Para las liberas requeridas, referirse a [requirementes.txt](requirements.txt).

## Uso de `data-presenter` 

### Como clase suelta:
```python
from data_presenter import DataPresenter
dp = DataPresenter('my_data.csv', alias='mis-datos')
dp.present('file')
with open('informe-mis-datos.md', 'w') as target:
    target.write(dp.presentation.read())
    dp.presentation.seek(0)
```

### Como script para analizar un conjunto de datasets:
Modificar el archivo [urls-datasets-portal.yaml](urls-datasets-portal.yaml) a gusto. Luego,
```bash
$ python data_presenter.py
```

Refiéras a la documentación de cada método del módulo para mayores detalles.

## Créditos 

Este prototipo de presentador de datos está fuertemente inspirado en la función `csvstat` de  [`csvkit`](https://csvkit.readthedocs.io/en/1.0.1/), una muy completa librería para manipular CSVs con Python o la línea de comandos.

## Contacto
Te invitamos a [crearnos un issue](https://github.com/datosgobar/data-presenter/issues/new?title=Encontré%20un%20bug%20en%20data-presenter) en caso de que encuentres algún bug o tengas comentarios de alguna parte de `data-presenter`. Para todo lo demás, podés mandarnos tu sugerencia o consulta a [datos@modernizacion.gob.ar](mailto:datos@modernizacion.gob.ar).
