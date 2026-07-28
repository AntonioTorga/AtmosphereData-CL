# AtmosphereData-CL

Software para **descargar productos de datos atmosféricos** —redes de observación
hoy; modelos y redes de monitoreo. Se usa por línea de comandos o como librería de Python.

Cambiar de formato (`.csv`, `.netcdf`, …) y cambiar de forma (un archivo por estación, un master único, un directorio particionado por fecha).

La idea es poder tanto crear como extender repositorios de datos, transformar entre formatos y formas, etc. Actualmente enfocado en datos de redes de monitoreo Chilenas como SINCA, DGA (datos de su visualizador VIPNET) y DMC (todas disponibles actualmente).

## Instalación

Clonar el repositorio y, dentro de un entorno virtual:

```
pip install -e .
```

El comando queda disponible como `AtmosphereData-CL`.

## Fuentes disponibles

Cada fuente es **una forma de acceso**, no una organización. Se listan con
`AtmosphereData-CL sources`.

| Fuente | Tipo | Auth | Productos / resolución | Notas |
|---|---|---|---|---|
| `vipnet` | PointSurface | — | Temperatura, Precipitación, Humedad, Viento, Nieve, Embalse (horario) | Metadatos de estación (lat/lon/nombre) incluidos |
| `sinca` | PointSurface | — | Contaminantes (O3, PM25, PM10, NO2, …) y meteorológicas; rango libre | lat/lon aún no (sí nombre/región) |
| `dmc-api` | PointSurface | usuario + token | Red EMA, por estación y mes | Requiere credenciales (ver abajo) |

### Credenciales de DMC

`dmc-api` necesita `usuario` y `token` de meteochile.gob.cl. Se toman de las
variables de entorno `DMC_API_USER` / `DMC_API_TOKEN` (se carga automáticamente un
archivo `.env` si existe), o se pasan con `--user` / `--token`.

```
# .env
DMC_API_USER=tu_usuario
DMC_API_TOKEN=tu_token
```

## Uso por línea de comandos

```
AtmosphereData-CL sources        # fuentes registradas
AtmosphereData-CL stores         # formas/formatos de almacenamiento
```

### `fetch` — descargar (y opcionalmente componer un master)

```
AtmosphereData-CL fetch FUENTE PRODUCTO PERIODO [opciones]
```

`PERIODO` usa **granularidad descendente** (año → mes → día), con separadores `-`
o `/` y campos de uno o dos dígitos. Acepta un año, mes, día o instante, o un
rango con `to` / ` - `:
`"2024"`, `"2024-01"`, `"2024-01-15"`, `"2026-07-20 12:00"`,
`"2024-01-01 to 2024-01-31"`.

Descargar a la *raw store* (que también es la caché — no re-descarga lo ya
guardado):

```
AtmosphereData-CL fetch vipnet Temperatura "2026-07-20 12:00"
```

Descargar y componer un master NetCDF:

```
AtmosphereData-CL fetch vipnet Temperatura "2026-07-20 12:00" \
    --to single-netcdf --dest ./salida
```

Caso de producción (cron) — **crecer** un master existente en vez de sobrescribir:

```
AtmosphereData-CL fetch vipnet Temperatura "2026-07-20 13:00" \
    --to single-netcdf --dest ./salida --append
```

El `--append` es idempotente (re-ejecutar el mismo periodo no duplica) y atómico
(un corte a mitad de escritura no corrompe el master).

`PRODUCTO` acepta una variable o una lista separada por comas (`O3,NO2,PM10`), que
se descargan en la misma llamada.

Opciones útiles: `--stations a,b,c`, `--raw-dir DIR`, `--extra clave=valor`
(opciones específicas de la fuente, p.ej. `--extra min_validation_level=preliminar`
en SINCA), `--user`/`--token` (dmc-api).

### `convert` — reformatear / reformar un almacén

```
AtmosphereData-CL convert FORMA_ORIGEN DIR_ORIGEN FORMA_DESTINO DIR_DESTINO [--append]
```

```
# de master único a un archivo por estación
AtmosphereData-CL convert single-netcdf ./salida one-csv-per-station ./por_estacion
```

## Uso como librería

```python
from atmosphere_data_cl.sources import Vipnet
from atmosphere_data_cl.store import SingleNetcdf, Store

# Descargar → devuelve un RawStore (nada se parsea hasta que se lee/convierte)
raw = Vipnet().fetch("Temperatura", "2026-07-20 12:00")

ds = raw.read()                                   # -> xarray.Dataset (time, station)
Store.change_format(raw, SingleNetcdf("salida"))          # componer master.nc
Store.change_format(raw, SingleNetcdf("salida"), mode="append")   # crecerlo

# Leer datos crudos ya en disco, sin red, usando el parser de la fuente:
from atmosphere_data_cl.store import RawStore
RawStore(Vipnet, "raw/vipnet").read()
```

## Formatos y formas (Stores)

Un *Store* es `directorio base` + `plantilla de ruta` + `formato`. La plantilla es
la forma y el codificador es el formato.

| Store | Forma |
|---|---|
| `single-netcdf` | un master `.nc` con todas las estaciones y variables |
| `one-csv-per-station` | un `.csv` por estación (+ `stations.csv` con metadatos) |

En NetCDF los metadatos de estación (lat/lon/nombre) viajan como coordenadas; en
los formatos tabulares van en un archivo `stations.csv` aparte.

## Arquitectura

- [AGENTS.md](AGENTS.md) — visión general y decisiones de diseño.
- [EXPLANATION.md](EXPLANATION.md) — cómo funcionan `Source` y `Store` en detalle, con un ejemplo completo.

## Estado

Implementado: descarga de `vipnet`, `sinca` y `dmc-api`; masters `.nc` y
`.csv`-por-estación; masters incrementales. 
