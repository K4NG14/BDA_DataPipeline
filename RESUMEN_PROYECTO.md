# Resumen Del Proyecto

## Objetivo

El proyecto se ha adaptado para construir una pipeline end-to-end sobre Barcelona con foco en turismo, alojamiento y precios de Airbnb.

Los objetivos actuales son:

- integrar varias fuentes heterogeneas en una arquitectura por zonas
- normalizar y limpiar los datos
- construir una base analitica reutilizable
- analizar como las zonas de Barcelona se relacionan con el precio y las caracteristicas de Airbnb mediante clustering

## Datasets Integrados

Se han incorporado las siguientes fuentes:

- `opendatabcn_pics-csv.csv`
  Contiene puntos de interes, equipamientos y atributos de localizacion.
- `opendatabcn_allotjament_hotels-csv.csv`
  Contiene hoteles de Barcelona y su categoria.
- `dataset_prat_temps_2025-26.csv`
  Serie temporal meteorologica.
- `listings.csv`
  Dataset principal de Airbnb con precio, tipo de alojamiento, capacidad, reviews y coordenadas.
- `neighbourhoods.csv`
  Relacion barrio-distrito para Airbnb.
- `reviews_airbnb.csv`
  Reviews de Airbnb. En la pipeline no se guarda review a review, sino que se agregan por `listing_id`.

## Script Principal

Toda la logica esta implementada en:

- [project_pipeline.py](/Users/joelalfaro/Documents/UPC/Q6/BDA/PROYECTO/project_pipeline.py)

## Explicacion Detallada De Las Funciones

Esta seccion resume el papel de las funciones principales del script y como se relacionan entre si.

### Funciones De Infraestructura

#### `ensure_dirs()`

Crea toda la estructura de carpetas del proyecto bajo `project_output`:

- `landing`
- `formatted`
- `trusted`
- `exploitation`
- `analysis`
- `reports`

Su objetivo es garantizar que cualquier escritura posterior tenga un directorio valido.

#### `utc_run_id()`

Genera un identificador temporal en UTC para versionar las ejecuciones. Se usa sobre todo en `landing`.

#### `canonical_name(name)`

Normaliza nombres de columnas:

- pasa a minusculas
- reemplaza caracteres no alfanumericos por `_`
- elimina repeticiones de `_`

Esto se usa al cargar CSV para unificar encabezados.

### Funciones De Parseo Y Limpieza Basica

#### `strip_text(value)`

Limpia cadenas:

- elimina BOM
- elimina espacios
- convierte `None` en cadena vacia

#### `parse_float(value)`

Convierte texto a `float`. Si no puede parsear, devuelve `None`.

#### `parse_int(value)`

Convierte texto numerico a entero a partir de `parse_float`.

#### `parse_bool(value)`

Convierte etiquetas tipo `true/false`, `t/f`, `1/0`, `yes/no` a `1` o `0`.

#### `parse_dt(value, fmt=None)`

Convierte fechas a formato ISO:

- usa un formato concreto si se pasa `fmt`
- si no, intenta interpretar el texto como ISO

#### `parse_price(value)`

Convierte precios tipo `$210.00` o `€210,00` en numerico.

### Funciones De Entrada/Salida

#### `load_rows(path, encoding)`

Lee un CSV completo y devuelve una lista de diccionarios con columnas ya normalizadas mediante `canonical_name`.

#### `write_csv(path, rows)`

Escribe una lista de diccionarios a CSV.

#### `write_csv_stream(path, fieldnames, rows)`

Escribe un CSV en streaming fila a fila. Esta funcion es clave para `calendar.csv`, ya que evita cargar millones de filas en memoria.

#### `write_json(path, payload)`

Escribe estructuras Python en JSON formateado.

#### `infer_duckdb_type(values)`

Infiera el tipo de columna en DuckDB:

- `BIGINT`
- `DOUBLE`
- `TEXT`

#### `write_duckdb_table(db_path, table_name, rows)`

Crea o reemplaza una tabla en DuckDB a partir de una lista de diccionarios ya cargada en memoria.

#### `write_duckdb_table_from_csv(db_path, table_name, csv_path)`

Crea o reemplaza una tabla en DuckDB leyendo un CSV directamente con `read_csv_auto`. Es la opcion adecuada para tablas muy grandes como `airbnb_calendar`.

### Funciones De Ingestion

#### `run_collectors(run_id)`

Implementa los data collectors de la `landing zone`:

- copia cada dataset original
- lo guarda en una carpeta por fuente
- le añade prefijo temporal
- genera `landing_manifest.json`

### Funciones De Normalizacion Por Dataset

#### `normalize_pics(rows)`

Transforma `opendatabcn_pics-csv.csv` en una tabla tabular consistente con:

- identificador
- nombre
- distrito y barrio
- atributo semantico
- coordenadas
- fechas

#### `normalize_hotels(rows)`

Transforma el dataset de hoteles a una estructura comun con:

- identificador
- nombre
- distrito y barrio
- categoria
- telefono
- coordenadas
- fechas

#### `normalize_weather(rows)`

Transforma la serie meteorologica:

- parsea timestamp
- deriva `date`
- convierte medidas a numerico
- conserva la fuente original

#### `normalize_airbnb_listings(rows)`

Transforma `listings.csv` al esquema base de Airbnb:

- identificador del listing
- zona
- coordenadas
- tipo de propiedad
- tipo de habitacion
- capacidad
- precio
- disponibilidad
- reviews
- reputacion
- metrica del host

#### `normalize_airbnb_calendar_row(row)`

Normaliza una fila individual de `calendar.csv`:

- `listing_id`
- `date`
- `available`
- `price`
- `adjusted_price`
- `minimum_nights`
- `maximum_nights`

Se define por fila porque `calendar.csv` es demasiado grande para cargarse completo de golpe.

#### `process_airbnb_calendar_formatted(spark)`

Procesa `calendar.csv` para la `formatted zone`:

- lo lee en streaming
- escribe `airbnb_calendar.csv`
- si hay Spark activo, genera tambien salidas Spark en CSV y Parquet
- registra la tabla `airbnb_calendar` en DuckDB

#### `normalize_airbnb_neighbourhoods(rows)`

Transforma `neighbourhoods.csv` en una tabla de correspondencia:

- `district_name`
- `neighborhood_name`

#### `aggregate_airbnb_reviews(path, encoding)`

Agrega `reviews_airbnb.csv` por `listing_id`:

- cuenta reviews
- calcula longitud media del comentario
- obtiene la fecha maxima de review

No guarda las reviews crudas, sino un resumen por alojamiento.

### Funciones De Formatted Zone

#### `run_spark_formatter(spark, formatted_rows)`

Recibe las tablas normalizadas ya en memoria y:

- crea DataFrames Spark
- escribe una version Spark CSV
- escribe una version Spark Parquet

Es la funcion que materializa la `formatted zone` con Spark.

#### `run_formatted_zone(engine='spark')`

Orquesta toda la `formatted zone`:

- carga datasets crudos
- normaliza todas las tablas
- obliga a usar Spark
- ejecuta `run_spark_formatter`
- procesa `airbnb_calendar`
- escribe tambien las tablas en `formatted_zone.duckdb`
- genera `formatted_zone_report.json`

### Funciones De Calidad Y Trusted Zone

#### `is_barcelona_coordinate(latitude, longitude)`

Valida si unas coordenadas caen dentro de un bounding box aproximado de Barcelona.

#### `evaluate_and_clean_formatted(formatted_rows)`

Aplica reglas de calidad a todas las tablas de `formatted`:

- elimina duplicados
- valida identidades
- valida distrito y barrio
- valida coordenadas
- valida rangos de clima
- valida precio y capacidad Airbnb

Devuelve:

- tablas limpias
- resumen de calidad por dataset

#### `run_trusted_zone(formatted_rows)`

Orquesta la `trusted zone`:

- ejecuta `evaluate_and_clean_formatted`
- escribe las tablas limpias en CSV
- escribe tablas limpias en `trusted_zone.duckdb`
- procesa `airbnb_calendar` filtrando solo listings validos
- genera `trusted_zone_quality_report.json`

### Funciones De Agregacion Analitica

#### `aggregate_weather_daily(weather_rows)`

Agrega la serie meteorologica a nivel diario.

#### `build_district_profile(trusted_rows)`

Construye un perfil turistico por distrito combinando:

- hoteles
- puntos de interes

#### `build_neighborhood_tourism_profile(trusted_rows)`

Construye un perfil turistico por barrio combinando:

- hoteles
- puntos de interes
- mapeo barrio-distrito Airbnb

#### `median(values)`

Devuelve la mediana de una lista numerica.

#### `build_airbnb_listing_enriched(trusted_rows, district_profile, neighborhood_profile)`

Es el join principal del proyecto a nivel de listing. Parte de `airbnb_listings` y lo enriquece con:

- reviews agregadas por `listing_id`
- perfil turistico del barrio
- perfil turistico del distrito

#### `build_airbnb_zone_features(listing_rows)`

Agrega `airbnb_listing_enriched` por `(district_name, neighborhood_name)` para crear una tabla de zonas.

Calcula:

- precios medios y medianos
- ratings
- capacidad
- disponibilidad
- ratios de tipo de alojamiento
- ratios de host
- indicadores turisticos

#### `build_district_day_features(district_profile, weather_daily)`

Cruza:

- perfil de distrito
- clima diario

Genera una tabla `distrito-dia`.

#### `build_airbnb_zone_day_features(trusted_rows, airbnb_listing_enriched, weather_daily)`

Construye la base final unificada `zona-dia`:

- une listings Airbnb validos
- usa `calendar.csv` para disponibilidad y precio por fecha
- añade perfil turistico por barrio y distrito
- añade la meteorologia diaria

Esta es la tabla mas cercana a una base unificada final con componente espacial y temporal.

### Funciones De Exploitation Zone

#### `run_exploitation_zone(trusted_rows)`

Orquesta la `exploitation zone`:

- crea perfiles por distrito
- crea perfiles por barrio
- agrega clima diario
- construye `district_day_features`
- construye `airbnb_listing_enriched`
- construye `airbnb_zone_features`
- construye `airbnb_zone_day_features`
- guarda todo en CSV y en `exploitation_zone.duckdb`

### Funciones De Visualizacion

#### `save_plot(path, title, x_values, y_values, kind='bar')`

Genera graficos simples si `matplotlib` esta disponible.

#### `run_visualization_pipeline(exploitation_rows)`

Genera un resumen descriptivo:

- ranking de zonas con mayor precio medio
- export de resultados a CSV
- graficos si la libreria grafica esta disponible

### Funciones Del Clustering

#### `mean(values)`

Calcula la media aritmetica.

#### `euclidean_distance(a, b)`

Calcula distancia euclidea entre dos vectores.

#### `standardize_matrix(matrix)`

Estandariza columnas de una matriz numerica:

- resta la media
- divide por la desviacion estandar

#### `run_kmeans(matrix, k=4, max_iter=40)`

Implementa un `k-means` sencillo en Python:

- inicializa centroides
- reasigna filas al centroide mas cercano
- recalcula centroides
- devuelve etiquetas e inercia

#### `describe_clusters(rows, labels)`

Resume cada cluster:

- numero de zonas
- distritos presentes
- precio medio
- rating medio
- capacidad media
- intensidad turistica media

#### `run_clustering_pipeline(exploitation_rows)`

Ejecuta el analisis principal del proyecto:

- toma `airbnb_zone_features`
- filtra zonas con suficiente numero de listings
- selecciona variables relevantes
- estandariza
- ejecuta `k-means`
- escribe `airbnb_zone_clusters.csv`
- escribe `cluster_summary.json`

### Funciones Finales De Orquestacion

#### `write_project_summary(run_id, analysis_outputs)`

Genera `project_summary.json` con:

- identificador de ejecucion
- rutas de zonas
- resultados del analisis

#### `run_all(engine)`

Ejecuta toda la pipeline completa en orden:

1. landing
2. formatted
3. trusted
4. exploitation
5. analysis

#### `parse_args()`

Lee argumentos CLI. Actualmente fuerza `--engine spark`.

#### `main()`

Punto de entrada del script. Llama a `parse_args()` y luego a `run_all()`.

## Tecnologia Usada

- `Spark / PySpark`
  Obligatorio en la `formatted zone`, tal como pide el PDF.
- `DuckDB`
  Base de datos fisica para almacenar tablas de `formatted`, `trusted` y `exploitation`.
- `Python`
  Orquestacion, limpieza, agregacion y analisis.

Nota importante:

- La `formatted zone` ya se ha ejecutado con Spark.
- Esto queda reflejado en [formatted_zone_report.json](/Users/joelalfaro/Documents/UPC/Q6/BDA/PROYECTO/project_output/reports/formatted_zone_report.json) con `engine_used = spark`.

## Arquitectura De Zonas

La pipeline esta dividida en:

1. `landing`
2. `formatted`
3. `trusted`
4. `exploitation`
5. `analysis`

## 1. Landing Zone

### Funcion

Se copian los datasets originales a una zona de aterrizaje versionada por timestamp. Esta capa conserva el dato de entrada sin transformacion.

### Estructura

Se crean carpetas separadas por fuente:

- `project_output/landing/pics/`
- `project_output/landing/hotels/`
- `project_output/landing/weather/`
- `project_output/landing/airbnb_listings/`
- `project_output/landing/airbnb_neighbourhoods/`
- `project_output/landing/airbnb_reviews/`

Cada fichero se versiona con un prefijo temporal.

### Metadatos

- [landing_manifest.json](/Users/joelalfaro/Documents/UPC/Q6/BDA/PROYECTO/project_output/reports/landing_manifest.json)

## 2. Formatted Zone

### Funcion

Esta zona transforma cada fuente a un esquema tabular consistente y comun. Es la capa de estandarizacion.

### Tecnologia

- obligatoriamente `Spark`
- salida adicional en `DuckDB`

### Artefactos

- [formatted_zone.duckdb](/Users/joelalfaro/Documents/UPC/Q6/BDA/PROYECTO/project_output/formatted/formatted_zone.duckdb)
- carpetas Spark en CSV y Parquet dentro de `project_output/formatted/...`

### Tablas De La Base De Datos

#### `pics`

Representa puntos de interes y atributos asociados.

Campos principales:

- `record_id`
- `name`
- `district_id`
- `district_name`
- `neighborhood_id`
- `neighborhood_name`
- `road_name`
- `zip_code`
- `town`
- `attribute_category`
- `attribute_name`
- `attribute_value`
- `main_address`
- `latitude`
- `longitude`
- `created_at`
- `modified_at`

#### `hotels`

Representa hoteles y su clasificacion.

Campos principales:

- `record_id`
- `name`
- `district_id`
- `district_name`
- `neighborhood_id`
- `neighborhood_name`
- `road_name`
- `zip_code`
- `town`
- `category_name`
- `category_path`
- `phone_label`
- `phone_value`
- `main_address`
- `latitude`
- `longitude`
- `created_at`
- `modified_at`

#### `weather`

Serie meteorologica homogeneizada.

Campos principales:

- `record_id`
- `timestamp`
- `date`
- `record_index`
- `wind_speed_avg`
- `wind_direction_avg`
- `wind_speed_max`
- `wind_direction_smm`
- `pressure_avg`
- `temperature_avg`
- `humidity_avg`
- `radiation_avg`
- `rain_total`
- `source_file`

#### `airbnb_listings`

Tabla principal para el analisis de precios.

Campos principales:

- `listing_id`
- `name`
- `district_name`
- `neighborhood_name`
- `neighbourhood_raw`
- `latitude`
- `longitude`
- `property_type`
- `room_type`
- `accommodates`
- `bathrooms`
- `bathrooms_text`
- `bedrooms`
- `beds`
- `price`
- `minimum_nights`
- `maximum_nights`
- `availability_365`
- `number_of_reviews`
- `reviews_per_month`
- `review_scores_rating`
- `review_scores_location`
- `review_scores_value`
- `host_is_superhost`
- `host_identity_verified`
- `instant_bookable`
- `calculated_host_listings_count`
- `estimated_occupancy_l365d`
- `estimated_revenue_l365d`
- `last_scraped`
- `last_review`

#### `airbnb_neighbourhoods`

Tabla de correspondencia barrio-distrito.

Campos:

- `district_name`
- `neighborhood_name`

#### `airbnb_reviews`

No almacena reviews individuales, sino un agregado por listing.

Campos:

- `listing_id`
- `review_count`
- `avg_comment_length`
- `last_review_date`

### Estructura Fisica Spark

Para cada dataset se generan:

- `project_output/formatted/<dataset>/csv/`
- `project_output/formatted/<dataset>/parquet/`

### Metadatos

- [formatted_zone_report.json](/Users/joelalfaro/Documents/UPC/Q6/BDA/PROYECTO/project_output/reports/formatted_zone_report.json)

## 3. Trusted Zone

### Funcion

Se aplican reglas de calidad, eliminacion de duplicados y validaciones semanticas. Esta capa mantiene las mismas tablas que `formatted`, pero con datos depurados.

### Base De Datos

- [trusted_zone.duckdb](/Users/joelalfaro/Documents/UPC/Q6/BDA/PROYECTO/project_output/trusted/trusted_zone.duckdb)

### Tablas

Se preservan las mismas tablas:

- `pics`
- `hotels`
- `weather`
- `airbnb_listings`
- `airbnb_neighbourhoods`
- `airbnb_reviews`

### Reglas De Calidad Aplicadas

#### `pics`

- identidad obligatoria
- distrito obligatorio
- coordenadas validas dentro de Barcelona
- eliminacion de duplicados por `(record_id, attribute_name, attribute_value)`

#### `hotels`

- identidad obligatoria
- distrito obligatorio
- coordenadas validas
- eliminacion de duplicados por `record_id`

#### `weather`

- timestamp obligatorio
- eliminacion de duplicados por timestamp
- validacion de temperatura
- validacion de humedad
- validacion de presion
- validacion de radiacion
- validacion de precipitacion

#### `airbnb_neighbourhoods`

- distrito obligatorio
- barrio obligatorio
- eliminacion de duplicados

#### `airbnb_reviews`

- agregacion unica por `listing_id`
- descarte de filas sin identificador

#### `airbnb_listings`

- identidad obligatoria
- barrio obligatorio
- distrito obligatorio
- imputacion de distrito desde `airbnb_neighbourhoods` cuando es posible
- coordenadas validas en Barcelona
- precio valido
- capacidad valida
- rating valido si existe
- eliminacion de duplicados por `listing_id`

### Metadatos

- [trusted_zone_quality_report.json](/Users/joelalfaro/Documents/UPC/Q6/BDA/PROYECTO/project_output/reports/trusted_zone_quality_report.json)

## 4. Exploitation Zone

### Funcion

Se construyen datasets analiticos ya integrados para consumo de negocio o modelos.

### Base De Datos

- [exploitation_zone.duckdb](/Users/joelalfaro/Documents/UPC/Q6/BDA/PROYECTO/project_output/exploitation/exploitation_zone.duckdb)

### Tablas De La Base De Datos

#### `district_profile`

Perfil turistico agregado a nivel de distrito.

Campos:

- `district_name`
- `hotels_count`
- `pics_count`
- `neighborhood_count`
- `top_hotel_category`
- `tourism_asset_score`

Interpretacion:

- resume la intensidad hotelera y de puntos de interes por distrito
- `tourism_asset_score = hotels_count * 2 + pics_count`

#### `neighborhood_profile`

Perfil turistico agregado a nivel de barrio.

Campos:

- `district_name`
- `neighborhood_name`
- `pics_count`
- `hotels_count`
- `tourism_asset_score`

#### `weather_daily`

Agregacion diaria de la serie meteorologica.

Campos:

- `date`
- `temperature_avg`
- `humidity_avg`
- `pressure_avg`
- `radiation_avg`
- `wind_speed_avg`
- `rain_total`
- `records_per_day`

#### `district_day_features`

Cruce entre perfil de distrito y meteorologia diaria.

Campos:

- `date`
- `district_name`
- `hotels_count`
- `pics_count`
- `neighborhood_count`
- `tourism_asset_score`
- `temperature_avg`
- `humidity_avg`
- `pressure_avg`
- `radiation_avg`
- `wind_speed_avg`
- `rain_total`

#### `airbnb_listing_enriched`

Tabla principal a nivel de listing, enriquecida con informacion territorial y turistica.

Bloques de columnas:

- identificacion:
  `listing_id`, `name`
- localizacion:
  `district_name`, `neighborhood_name`, `latitude`, `longitude`
- caracteristicas del alojamiento:
  `property_type`, `room_type`, `accommodates`, `bathrooms`, `bedrooms`, `beds`
- precio y disponibilidad:
  `price`, `minimum_nights`, `maximum_nights`, `availability_365`
- reviews y reputacion:
  `number_of_reviews`, `reviews_per_month`, `review_scores_rating`, `review_scores_location`, `review_scores_value`
- host:
  `host_is_superhost`, `host_identity_verified`, `instant_bookable`, `calculated_host_listings_count`
- negocio:
  `estimated_occupancy_l365d`, `estimated_revenue_l365d`
- reviews agregadas:
  `review_count_from_reviews_csv`, `avg_comment_length`
- senal turistica del barrio:
  `neighborhood_pics_count`, `neighborhood_hotels_count`, `neighborhood_tourism_asset_score`
- senal turistica del distrito:
  `district_pics_count`, `district_hotels_count`, `district_tourism_asset_score`

#### `airbnb_zone_features`

Tabla agregada por `(district_name, neighborhood_name)` y principal entrada del clustering.

Campos:

- `district_name`
- `neighborhood_name`
- `listing_count`
- `avg_price`
- `median_price`
- `avg_rating`
- `avg_accommodates`
- `avg_bedrooms`
- `avg_availability_365`
- `avg_reviews_per_month`
- `entire_home_ratio`
- `private_room_ratio`
- `superhost_ratio`
- `instant_bookable_ratio`
- `avg_review_count`
- `avg_comment_length`
- `neighborhood_pics_count`
- `neighborhood_hotels_count`
- `neighborhood_tourism_asset_score`
- `district_tourism_asset_score`

Interpretacion:

- cada fila representa una zona de Airbnb
- mezcla variables de precio, composicion de oferta, reputacion y entorno turistico

## 5. Analysis

### Objetivo Analitico

Estudiar como las zonas de Barcelona afectan al precio y al perfil de los alojamientos Airbnb.

### Analisis 1: Visualizacion Descriptiva

Se genera un ranking de zonas por precio medio.

Salida principal:

- [airbnb_zone_top10.csv](/Users/joelalfaro/Documents/UPC/Q6/BDA/PROYECTO/project_output/analysis/visualization/airbnb_zone_top10.csv)

Informacion destacada de la ultima ejecucion:

- aparecen zonas caras en `Sant Marti` y `Eixample`
- ejemplos: `Diagonal Mar i el Front Maritim del Poblenou`, `la Dreta de l'Eixample`, `Sant Antoni`

### Analisis 2: Clustering De Zonas Airbnb

Se ha sustituido el analisis anterior de regresion por un clustering de zonas Airbnb.

#### Unidad de analisis

- una zona = una fila de `airbnb_zone_features`
- se clusterizan solo zonas con suficiente volumen de listings

#### Variables usadas en el clustering

- `avg_price`
- `median_price`
- `avg_rating`
- `avg_accommodates`
- `avg_bedrooms`
- `avg_availability_365`
- `entire_home_ratio`
- `private_room_ratio`
- `superhost_ratio`
- `instant_bookable_ratio`
- `avg_review_count`
- `neighborhood_tourism_asset_score`
- `district_tourism_asset_score`

#### Metodo

- estandarizacion de variables
- clustering tipo `k-means`
- `k = 4`

#### Salidas

- [airbnb_zone_clusters.csv](/Users/joelalfaro/Documents/UPC/Q6/BDA/PROYECTO/project_output/analysis/clustering/airbnb_zone_clusters.csv)
- [cluster_summary.json](/Users/joelalfaro/Documents/UPC/Q6/BDA/PROYECTO/project_output/analysis/clustering/cluster_summary.json)

#### Resultado de la ultima ejecucion

- listings Airbnb enriquecidos: `15256`
- zonas agregadas: `71`
- zonas usadas en clustering: `55`
- numero de clusters: `4`
- inercia: `311.305901`

#### Lectura de clusters

##### Cluster 0

- zonas: `7`
- distritos destacados: `Ciutat Vella`, `Gracia`, `Sarria-Sant Gervasi`
- precio medio aproximado: `140.29`
- alta intensidad turistica media
- muchos listings por zona

##### Cluster 1

- zonas: `28`
- cluster mas grande
- precio medio aproximado: `147.61`
- intensidad turistica media-baja
- mezcla de distritos mas residenciales y perifericos

##### Cluster 2

- zonas: `11`
- precio medio aproximado: `81.88`
- menor capacidad media
- baja intensidad turistica
- representa zonas mas economicas

##### Cluster 3

- zonas: `9`
- distritos destacados: `Eixample` y `Sant Marti`
- precio medio aproximado: `223.34`
- mayor capacidad media
- mayor valor de mercado

### Conclusiones Del Analisis

- el precio no depende solo del distrito, sino del barrio concreto
- la senal turistica del entorno ayuda a separar zonas premium de zonas residenciales
- `Eixample` y partes de `Sant Marti` concentran las zonas con mayor precio medio
- existen zonas turisticas consolidadas con precio intermedio pero gran densidad de oferta
- los clusters mas baratos suelen combinar menor capacidad media y menor intensidad turistica

## Archivos Relevantes

- [project_pipeline.py](/Users/joelalfaro/Documents/UPC/Q6/BDA/PROYECTO/project_pipeline.py)
- [RESUMEN_PROYECTO.md](/Users/joelalfaro/Documents/UPC/Q6/BDA/PROYECTO/RESUMEN_PROYECTO.md)
- [project_summary.json](/Users/joelalfaro/Documents/UPC/Q6/BDA/PROYECTO/project_output/reports/project_summary.json)
- [landing_manifest.json](/Users/joelalfaro/Documents/UPC/Q6/BDA/PROYECTO/project_output/reports/landing_manifest.json)
- [formatted_zone_report.json](/Users/joelalfaro/Documents/UPC/Q6/BDA/PROYECTO/project_output/reports/formatted_zone_report.json)
- [trusted_zone_quality_report.json](/Users/joelalfaro/Documents/UPC/Q6/BDA/PROYECTO/project_output/reports/trusted_zone_quality_report.json)
- [formatted_zone.duckdb](/Users/joelalfaro/Documents/UPC/Q6/BDA/PROYECTO/project_output/formatted/formatted_zone.duckdb)
- [trusted_zone.duckdb](/Users/joelalfaro/Documents/UPC/Q6/BDA/PROYECTO/project_output/trusted/trusted_zone.duckdb)
- [exploitation_zone.duckdb](/Users/joelalfaro/Documents/UPC/Q6/BDA/PROYECTO/project_output/exploitation/exploitation_zone.duckdb)

## Ejecucion

La ejecucion correcta, alineada con el PDF, es:

```bash
export JAVA_HOME="$("/opt/homebrew/bin/brew" --prefix openjdk@21)/libexec/openjdk.jdk/Contents/Home"
.venv/bin/python project_pipeline.py --engine spark
```

El uso de `Spark` es obligatorio para la `formatted zone`.
