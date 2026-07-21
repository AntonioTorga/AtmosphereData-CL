# AtmosphereData-CL

Software para descargar productos de datos atmosféricos —redes de observación, salidas de
modelos, datos satelitales— y transformarlos entre formatos y formas (*shapes*). La interfaz
de este software es vía línea de comandos, y también puede usarse como librería.

## Idea de este proyecto

Crear un software que pueda descargar y procesar automáticamente datos atmosféricos de
distintas fuentes, y escribirlos en el formato y la forma que se necesite:

- **Formatos**: `.csv`, `.json`, `.netcdf`, y otros a futuro (`.parquet`, `.zarr`). Si los
  datos ya vienen en el formato deseado, la conversión es una operación nula.
- **Formas**: un archivo maestro único, un directorio maestro particionado
  (`año/mes/día.ext`), un archivo por estación, un archivo por variable.

Cambiar de formato y cambiar de forma son la misma operación: leer de un almacén y escribir
en otro.

La meta es poder ejecutarlo diariamente con `crontab`, extendiendo los archivos maestros de
manera incremental sin reescribirlos completos.

## Estado actual

Por ahora está implementada la descarga de la red **DMC** (vía su API) y su procesamiento
desde el formato "intermedio" (data tabulada en formato wide, guardada en `.csv`) al formato
requerido por Melodies-MONET. El soporte para modelos y satélites, y la abstracción general
de fuentes y almacenes, están en desarrollo.

La arquitectura acordada está documentada en [AGENTS.md](AGENTS.md).

## Instalación

Para instalar debe clonar este repositorio y ejecutar dentro de un ambiente virtual

```
pip install -e .
```

## Uso

La interfaz de comandos puede ser accedida desde la terminal escribiendo `AtmosphereData-CL`.
Para obtener más información de su uso ejecute:

```
AtmosphereData-CL --help
```
