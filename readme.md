# 📦 Proyecto: Pipeline ELT Airbnb NYC — Batch + DWH + Airflow

## 🧭 Resumen general
Este proyecto implementa un **pipeline ELT** para **ingesta, transformación y almacenamiento** de datos en **procesamiento por lotes (batch)**. El flujo completo se ejecuta de forma **orquestada con Apache Airflow** y persiste los datos en un **Data Warehouse escalable y estructurado por capas** (staging → silver → gold).

### Objetivos principales
- **Diseñar un pipeline ELT** para la ingesta, transformación y almacenamiento de datos en **batch**.
- **Construir un Data Warehouse escalable** y **estructurado en capas** (medallón).
- **Orquestar** el pipeline end-to-end con **Apache Airflow**.
- **Implementar procesos de ingesta** mediante **scripts en Python** (extractores modulares, logging, control de calidad y verificación de archivos).
- **Aplicar transformaciones** sobre grandes volúmenes utilizando **SQL** (modelado dimensional con dbt).
- **Orquestar integralmente** la ingesta, extracción, transformación y persistencia mediante **workflows en Apache Airflow**.

---

## 📂 Sobre la estructura del repositorio (importante)
En la **raíz de este repositorio** encontrarás una carpeta llamada **[PRIMER AVANCE/](PRIMER%20AVANCE/)**.  
Esta carpeta contiene la **primera entrega** con el **diseño de la arquitectura general** para llevar el pipeline a la **nube (AWS)**. Ahí se documenta:

- Las **herramientas** a utilizar (contenedores, orquestación, almacenamiento, modelado).
- La **arquitectura objetivo en AWS**.
- La **estructura del proyecto** desde la **E (Extract)** hasta la **T (Transform)**, incluyendo **L (Load)**, con diagramas y justificación técnica.
- Consideraciones de **gobernanza, calidad de datos y escalabilidad**.

Puedes **visualizar esta primera parte** en el PDF dentro de esa carpeta:

- 📄 **PDF**: [`PRIMER AVANCE/Diseño de la arquitectura general del pipeline ELT AB_NYC.pdf`](PRIMER%20AVANCE/Dise%C3%B1o%20de%20la%20arquitectura%20general%20del%20pipeline%20ELT%20AB_NYC.pdf)

---

## 🏗️ Componentes del pipeline (visión rápida)
- **Extract (E)**: Scripts en **Python** para:
  - AB_NYC (CSV)
  - Banxico SIE (API FX USD/MXN)
  - Wikipedia (scraping de boroughs)
- **Load (L)**: Carga cruda a esquema **staging** y almacenamiento de archivos **raw** por fecha.
- **Transform (T)**: Modelado **dbt** en capas **staging → silver → gold** (incluye SCDs donde aplica).
- **Orquestación**: **Apache Airflow** (DAGs, dependencias)
- **Calidad de datos**: Reglas en Python + reportes por corrida (duplicados, rangos, dominios, nulidad).

# 🚀 Capa RAW – Pipeline de Extracción en Contenedor `extractor`

La **capa RAW** es la primera etapa del pipeline de datos.  
Aquí almacenamos los datos **sin transformar**, organizados por **fuente/año/mes/día**, junto con **metadatos, reportes de calidad y logs**.  

👉 Toda la extracción corre dentro de un **contenedor ligero** (`extractor`) definido en [`extractor.Dockerfile`](extractor.Dockerfile).  
👉 Los logs se registran tanto en **consola** como en el archivo [`logs`](logs\extractor.log).  

---

## 🏗️ Estructura del proyecto

proyecto_integrador_ETL/  
├── data/                # 📂 Carpeta de almacenamiento RAW  
├── logs/                # 📝 Logs del extractor  
│   └── extractor.log  
├── src/                 # 📦 Código fuente del pipeline  
│   ├── extract/         # 📥 Extractores de cada fuente  
│   │   ├── extract_csv.py  
│   │   ├── extract_banxico.py  
│   │   └── web_scraping_nyc.py  
│   ├── utils/           # ⚙️ Utilidades compartidas  
│   │   ├── config.py  
│   │   ├── logger.py  
│   │   ├── paths.py  
│   │   ├── quality.py  
│   │   └── verify.py  
│   └── main.py          # 🎯 Orquestador de la capa RAW  
├── inputs/              # 📂 Archivos CSV de entrada (ej: AB_NYC.csv)  
├── .env                 # 🌍 Configuración de entorno  
├── extractor.Dockerfile # 📦 Imagen ligera del extractor  
├── docker-compose.yml   # 🐳 Orquestación de servicios  
└── requirements.txt     # 📋 Dependencias mínimas  


---

## 📖 Explicación de los archivos principales

### 🔹 Carpeta `extract/`
- **[`extract_csv.py`](src\extract\extract_csv.py)** 📄  
  Copia el CSV `AB_NYC` desde `inputs/` a RAW.  
  - Calcula MD5 → si el archivo no cambió, **no lo copia** y registra referencia diaria en el manifest.  
  - Garantiza trazabilidad con **[`manifest_raw.jsonl`](data\status\verify\ab_nyc\manifest_raw.jsonl).**

- **[`extract_banxico.py`](src\extract\extract_banxico.py)** 💱  
  Descarga el tipo de cambio USD/MXN desde la API **Banxico SIE**.  
  - Normaliza en columnas: `fecha`, `valor`.  
  - Guarda en RAW con convención:  
    `data/raw/files/banxico/YYYY/MM/DD/banxico_<serie>_<timestamp>.csv`.
  - Calcula MD5 → si el archivo no cambió, **no lo copia** y registra referencia diaria en el manifest.  
  - Garantiza trazabilidad con **[`manifest_raw.jsonl`](data\status\verify\banxico\manifest_raw.jsonl).**    

- **`web_scraping_nyc.py`** 🌐  
  Scraping de Wikipedia → tabla de **boroughs de NYC**.  
  - Limpieza de columnas: `borough`, `population`, `land_area_km2`, `density_km2`.  
  - Valida que existan los 5 boroughs esperados.  
  - Guarda en RAW bajo convención:  
    `data/raw/files/nyc_boroughs/YYYY/MM/DD/nyc_boroughs_<timestamp>.csv`.
  - Calcula MD5 → si el archivo no cambió, **no lo copia** y registra referencia diaria en el manifest.  
  - Garantiza trazabilidad con **[`manifest_raw.jsonl`](data\status\verify\nyc_boroughs\manifest_raw.jsonl).** 

---

### 🔹 Carpeta `utils/`
- **[`config.py`](src\utils\config.py)** ⚙️  
  Carga `.env` automáticamente. Define variables como:  
  `RAW_DIR`, `BANXICO_SERIES_ID`, `RUN_SCRAPER_NYC`, `LOG_LEVEL`.  

- **[`logger.py`](src\utils\logger.py)** 📝  
  Logger único con formato:  
- Consola + archivo.  

- **[`paths.py`](src\utils\paths.py)** 📁  
Genera rutas estándar para RAW:  
`data/raw/files/<source>/<YYYY>/<MM>/<DD>`.  

- **[`quality.py`](src\utils\quality.py)** ✅  
Define **reglas de calidad de datos (DQ)** para cada fuente.  
- Ejemplo `banxico`: columna `valor` > 0 y fechas únicas.  
- Ejemplo `ab_nyc`: precios ≥ 0, `room_type` válido.  
- Genera reportes JSON en [`data/status/dq/<source>/...`](data\status\dq\banxico\2025\09\02\dq_banxico_20250902T194639Z.json).  

- **[`verify.py`](src\utils\verify.py)** 🔒  
- Calcula **MD5** y evita duplicados.  
- Registra en manifest: [`data/status/verify/<source>/manifest_raw.jsonl`](data\status\verify\banxico\manifest_raw.jsonl).  
- Soporta referencias diarias (si el archivo no cambió).  

---

### 🔹 Orquestador
- **[`main.py`](src\main.py)** 🎯  
Orquesta los tres extractores:  
1. **CSV → RAW** (con referencia diaria si no cambió).  
2. **Banxico → RAW** (siempre crea archivo y se valida).  
3. **Scraper NYC → RAW** (solo si `RUN_SCRAPER_NYC=1`).  

Después de cada paso:  
- Se valida DQ (`quality.py`).  
- Se aplica `_post_write` → existencia, tamaño, MD5, deduplicación.  
- Si falla un step en modo **soft** (`STRICT_MODE=0`), se genera un **artefacto de estado** en `data/status/extract/...`.

## 📑 Ejemplos de artefactos en la capa RAW

Para que quede claro cómo luce la información registrada en `status/`, aquí algunos ejemplos reales:

---

### 🔒 Manifest – `data/status/verify/banxico/manifest_raw.jsonl`

Este archivo almacena un registro por cada CSV guardado en RAW, incluyendo su **hash MD5** para evitar duplicados.

```json
{"ts_utc": "2025-09-04T12:00:00Z", "source": "banxico", "path": "data/raw/files/banxico/2025/09/04/banxico_SF43718_20250904T120000Z.csv", "md5": "5f3c8d9c7eae45f6d28c3f29a63b91de"}
{"ts_utc": "2025-09-04T12:05:00Z", "source": "banxico", "path": "data/raw/files/banxico/2025/09/04/banxico_SF43718_20250904T120500Z.csv", "md5": "5f3c8d9c7eae45f6d28c3f29a63b91de", "reference": true}
```

## ⚙️ Archivos de infraestructura
### 🌍 Ejemplo de `.env`

Este archivo configura rutas, tokens y flags de ejecución:

```env
# ============ Logging & rutas ============
LOG_LEVEL=INFO
RAW_DIR=./data/raw

# ============ CSV local de ejemplo ============
LOCAL_CSV_PATH=./inputs/AB_NYC.csv
LOCAL_CSV_SOURCE_NAME=ab_nyc

# ============ Banxico ============
BANXICO_SERIES_ID=SF43718
BANXICO_TOKEN=<tu_token_aqui>
BANXICO_SOURCE_NAME=banxico

# Política de fallo global: 0=soft-fail, 1=fail-fast
STRICT_MODE=0

# ============ Scraper Wikipedia (boroughs NYC) ============
RUN_SCRAPER_NYC=1
SCRAPER_NYC_SOURCE_NAME=nyc_boroughs
SCRAPER_NYC_URL=https://en.wikipedia.org/wiki/Boroughs_of_New_York_City

# ================ parámetros HTTP del scraper =================
HTTP_USER_AGENT=Integrador-ETL/1.0 (+educativo)
HTTP_TIMEOUT=30

# ============ Data Quality (DQ) ============
DQ_STRICT=0

# ================ Postgres (compose) ===================
POSTGRES_USER=nyc_user
POSTGRES_PASSWORD=nyc_pass
POSTGRES_DB=ab_nyc_dw
POSTGRES_HOST=postgres
POSTGRES_PORT=5433

# ==========airflow================
#AIRFLOW_DB=airflow
#TZ=America/Mexico_City

# ================== Conexión local para notebooks ===================
PGHOST=localhost
PGPORT=5433
PGDATABASE=ab_nyc_dw
PGUSER=nyc_user
PGPASSWORD=nyc_pass
PGSCHEMA=public  
```
## 🐳 Contenedores: cómo funciona el `extractor` y cómo se ejecuta

La capa RAW corre dentro de un **contenedor ligero** llamado `extractor`.  
El contenedor se construye con [`extractor.Dockerfile`](extractor.Dockerfile) y se orquesta desde [`docker-compose.yml`](docker-compose.yml).

---

### 🧱 ¿Qué hace [`extractor.Dockerfile`](extractor.Dockerfile)? (idea general)

- **Base ligera de Python** (slim): reduce tamaño de la imagen y el tiempo de build.
- **Instala solo lo necesario** desde [`requirements.txt`](requirements.txt) (pandas, bs4, dotenv).
- Define el **directorio de trabajo** `/app`.
- Copia el **código fuente** `src/`, el archivo `.env` (si aplica en build) y la carpeta `inputs/` (o bien se montan en runtime via compose).

## 🚀 Ciclo de ejecución — Capa RAW (en contenedor `extractor`)

### ✅ Prerrequisitos (una sola vez)
- Tener **Docker** y **docker compose** instalados.
- Archivo **`.env`** en la raíz con variables mínimas:
  - `RAW_DIR`, `LOG_LEVEL`
  - `LOCAL_CSV_PATH`, `LOCAL_CSV_SOURCE_NAME`
  - `BANXICO_SERIES_ID`, `BANXICO_TOKEN`
  - `RUN_SCRAPER_NYC` (0/1), `SCRAPER_NYC_URL`, `HTTP_USER_AGENT`, `HTTP_TIMEOUT`
- CSV de entrada disponible en `./inputs/` (ej. `AB_NYC.csv`).

---

### 🧱 1) Construir la imagen ligera
```bash
docker compose build extractor
```
### ▶️ 2) Ejecutar el pipeline RAW (one-shot)
```bash
 docker compose run --rm extractor
# (equivalente) docker compose run --rm extractor python -m src.main
```
### 📂 Verificar artefactos generados

```text
data/
├─ raw/
│  └─ files/
│     └─ <source>/
│        └─ <YYYY>/<MM>/<DD>/
│           └─ <source>_<timestamp>.csv
├─ status/
│  ├─ verify/
│  │  └─ <source>/manifest_raw.jsonl
│  ├─ dq/
│  │  └─ <source>/<YYYY>/<MM>/<DD>/
│  │     └─ dq_<source>_<timestamp>.json
│  └─ extract/
│     └─ <stage>/<stage>_<timestamp>.json   # solo si hubo fallo soft
└─ logs/
   └─ extractor.log                          
```

# 🧱 Capa de Transformación (dbt) — **Load** & **Transform**

Esta sección documenta **cómo pasamos de RAW → DWH** usando **dbt** (modelo medallón: *staging → silver → gold*), partiendo de *snapshots* diarios y **foreign tables** que apuntan a los `latest.csv`.  
Antes de transformar, el diseño fue definido en el cuaderno **[`notebook/01_modelado_dimensional.ipynb`](notebook\01_modelado_dimensional.ipynb)**, donde se describe el **modelo dimensional** (hechos y dimensiones) y preguntas de negocio. 

---

## 🧩 Visión general del flujo (con iconos)

```text
📥 RAW (archivos CSV por fuente, diarios)
   └── data/raw/files/<source>/<YYYY>/<MM>/<DD>/<source>_<timestamp>.csv

🔗 Symlinks "latest" (snapshot vigente del día)
   └── scripts/update_latest_symlinks.sh     # apunta <source>/latest.csv al CSV más reciente

🗄️ Postgres (file_fdw → FOREIGN TABLES)
   └── raw_ext.ab_nyc_latest         → data/raw/files/ab_nyc/latest.csv
   └── raw_ext.banxico_latest        → data/raw/files/banxico/latest.csv
   └── raw_ext.nyc_boroughs_latest   → data/raw/files/nyc_boroughs/latest.csv

🏗️ dbt SOURCES (leen raw_ext.*_latest)
   └── sources.yml  # define "raw_ext" como fuente

🧱 STAGING (tipificación, limpieza, snapshot keys)
   └── models/staging/*.sql
       • Normaliza tipos/columnas
       • Agrega:
         - snapshot_date (DATE)
         - snapshot_date_key (INT: YYYYMMDD)

📚 SNAPSHOTS (SCD-2 cuando aplica)
   └── snapshots/*.sql
       • Capturan cambios en el tiempo (por ejemplo listing, host)

🏛️ SILVER (dimensiones conformadas y hechos)
   └── models/silver/*.sql
       • Dimensiones: dim_borough, dim_neighbourhood, dim_room_type, dim_host, dim_listing, dim_exchange_rate, …
       • Hechos / auditorías: fct_listing_snapshot, fx_rate_audit, …

🥇 GOLD (métricas de negocio y agregados)
   └── models/gold/gq*.sql
       • gq1_price_by_area, gq7_price_distribution_outliers, gq9_borough_supply_density_ranked, …
```

---

## 📦 **Load** (cargar “instantánea del día”)

### 1) Mantener el **snapshot vigente**: `latest.csv`
El script [`scripts/update_latest_symlinks.sh`](scripts\update_latest_symlinks.sh) se ejecuta contra el contenedor de Postgres:
- Busca el **CSV más reciente** por fuente en `data/raw/files/<source>/YYYY/MM/DD/*.csv`.
- Actualiza el **symlink** `data/raw/files/<source>/latest.csv` para que siempre apunte al último archivo.

> Comando sugerido:  
> `bash scripts/update_latest_symlinks.sh`  
> *(si estás en Windows con Docker Desktop, también funciona al invocarlo desde Git Bash o WSL)*

---

### 2) Crear **bases de datos** y **foreign tables** (carpeta `sql/`)

- En [`sql/`](sql) se incluyen los scripts que **preparan Postgres**:
  - [`000_init.sh`](sql\000_init.sh): crea de forma idempotente las BDs del proyecto (ej. `ab_nyc_dw`, `airflow`). *(Ruta sugerida para el link: `sql/000_init.sh`)*  
  - [`010_raw_ext_foreign_tables.sql`](sql\010_raw_ext_foreign_tables.sql): crea el **schema** `raw_ext`, el **servidor** `csv_server` (`file_fdw`) y las **FOREIGN TABLES**:
    - `raw_ext.ab_nyc_latest` → `/data/raw/files/ab_nyc/latest.csv`
    - `raw_ext.banxico_latest` → `/data/raw/files/banxico/latest.csv`
    - `raw_ext.nyc_boroughs_latest` → `/data/raw/files/nyc_boroughs/latest.csv` 

> **Cómo se ejecutan:**  
> - Si `docker-compose.yml` monta `./sql:/docker-entrypoint-initdb.d:ro`, se aplican **automáticamente** al levantar Postgres.  
> - Alternativa manual: `docker compose exec postgres psql -U <user> -d <db> -f /path/en/contenedor/010_raw_ext_foreign_tables.sql`

---

## 🔧 **Transform** (dbt) — Medallón

### A) **Staging** (tipificación, normalización y snapshot keys)
- Los modelos de *staging* toman `raw_ext.*_latest` como **source**.  
- Ejemplo: [`models/staging/stg_ab_nyc.sql`](ab_nyc_dw\models\staging\stg_ab_nyc.sql):
  - Tipifica y normaliza columnas (`id` → `listing_id_nat`, `price` → `price_usd`, `last_review` → `last_review_date`, etc.).
  - **Enriquece con metadatos de snapshot**:
    - `snapshot_date` (date)
    - `snapshot_date_key` (int `YYYYMMDD`)
  - Estas claves se parametrizan vía `var('snapshot_date')` / `var('snapshot_date_key')` y por defecto toman la fecha de ejecución (`run_started_at`). 

> **Ejecución recomendada (solo staging):**  
> `docker compose run --rm dbt dbt run --select staging`

> **Opcional (fijar snapshot del día en una corrida histórica):**  
> `docker compose run --rm dbt dbt run --select staging --vars "snapshot_date: 2025-09-04, snapshot_date_key: 20250904"`

---

### B) **Silver** (dimensiones y hechos)
En *[silver](ab_nyc_dw\models\silver)* materializamos el **modelo dimensional** (dims y hechos) a partir de *staging* y/o *snapshots*.  
- `dim_borough.sql`, `dim_neighbourhood.sql`, `dim_room_type.sql`: **dimensiones conformadas** para enriquecer `ab_nyc`.  
- `dim_exchange_rate.sql`+`dim_date.sql` + `fx_rate_audit.sql`: tabla de **tipos de cambio** y trazabilidad/auditoría.  
- `dim_listing.sql`, `dim_host.sql`: entidades normalizadas desde staging usan SCD.
- `fct_listing_snapshot.sql`: **tabla de hechos** que representa el estado del *listing* en cada **snapshot_date** (grano *listing × snapshot*).  

> **[dbt snapshots](ab_nyc_dw\snapshots)** (carpeta `snapshots/`):  
> - Capturan cambios **a lo largo del tiempo** en entidades como *listing* y *host* (SCD-2).  
> - Se alimentan de *staging* y escriben en tablas “\_snapshots” que luego usa *silver*.  
> - Archivo en repo: [`snapshots/listing_snapshot.sql`](ab_nyc_dw\snapshots\listing_snapshot.sql), [`snapshots/host_snapshot.sql`](ab_nyc_dw\snapshots\host_snapshot.sql).

### 🔬 Tests en **Staging** y **Silver** (dbt)

Además de construir los modelos, **staging** y **silver** incluyen **tests** para asegurar calidad e integridad referencial.  

#### 📁 Dónde viven los tests
- **Staging**: [`ab_nyc_dw/models/staging/schema.yml`](ab_nyc_dw\models\staging\schema.yml)
- **Silver**: [`ab_nyc_dw/models/silver/schema.yml`](ab_nyc_dw\models\silver\schema.yml)

> **Ejecución recomendada (staging + snapshots + silver) con _tests_ incluidos:**
```bash
# 1) STAGING → build + tests
docker compose run --rm dbt sh -lc "dbt run --select staging && dbt test --select staging"

# 2) SNAPSHOTS (SCD-2)
docker compose run --rm dbt dbt snapshot

# 3) SILVER → build + tests
docker compose run --rm dbt sh -lc "dbt run --select silver && dbt test --select silver"
```
## C) 🥇**GOLD** — Métricas, KPIs y agregados (dbt)

La capa **gold** contiene **vistas** orientadas al análisis de negocio.  
Se construyen **encima de silver** (dimensiones conformadas y la fact **`fct_listing_snapshot`**) y, cuando aplica, de **`dim_date`** y **`dim_exchange_rate`** para manejar tiempos y FX.

---

### 📂 Estructura (rutas dentro de [`ab_nyc_dw/models/gold`](ab_nyc_dw\models\gold))

```text
ab_nyc_dw/
└─ models/
   └─ gold/
      ├─ gq1_price_by_area.sql
      ├─ gq2_roomtype_supply_revenue.sql
      ├─ gq3_top_hosts_pricing.sql
      ├─ gq4_availability_diffs.sql
      ├─ gq5_reviews_trend_monthly.sql
      ├─ gq6_active_listings_concentration.sql
      ├─ gq7_price_distribution_outliers.sql
      ├─ gq8_availability_vs_reviews.sql
      ├─ gq9_borough_supply_density_ranked.sql
      └─ schema.yml      # tests de la capa gold (not_null, unique, etc.)
```

> **Materialización**: las consultas gold se materializan como **`view`** (config en cada `.sql`).

---

### 🔎 Modelos clave (qué hace cada uno)

- **[`gq1_price_by_area.sql`](ab_nyc_dw\models\gold\gq1_price_by_area.sql)**  
  - Toma el **último snapshot** (`max(snapshot_date_key)`) de `fct_listing_snapshot`.  
  - Calcula precios **promedio por borough y neighbourhood** (USD).  
  - Agrega columnas revaluadas a **MXN** de dos formas:
    - **as-of**: usando el **FX vigente a la fecha del snapshot**.
    - **fx_latest**: usando el **último FX disponible** en `dim_exchange_rate`.  
  - Devuelve **rankings** por precio para comparar zonas.

- **[`gq2_roomtype_supply_revenue.sql`](ab_nyc_dw\models\gold\gq2_roomtype_supply_revenue.sql)**  
  - Agrega por **tipo de habitación**:  
    - **oferta activa** (conteo de listings activos).  
    - **proxy de revenue** en **MXN (as-of)** y **USD**, más **MXN revaluado** con **último FX**.  
  - Emplea un cálculo simple de **noches reservadas**: `365 - availability_365` (nunca negativo).  
  - Incluye **ranks** de popularidad y revenue.

- **[`gq3_top_hosts_pricing.sql`](ab_nyc_dw\models\gold\gq3_top_hosts_pricing.sql)**  
  - Ranking de **hosts** precio / oferta activa (sobre el último snapshot).  
  - Útil para detectar proveedores con mayor impacto.

- **[`gq4_availability_diffs.sql`](ab_nyc_dw\models\gold\gq4_availability_diffs.sql)**  
  - Promedio y **mediana (p50)** de **availability_365** por **borough** y **room_type** en el **último snapshot**.  
  - Permite ver **brechas** de disponibilidad entre zonas y tipos de habitación.

- **`gq5_* … gq9_*`**  
  - **Tendencias mensuales de reseñas**, **concentración de listings activos**, **distribución de precios / outliers**, **relación disponibilidad vs reseñas**, y **ranking por densidad de oferta**.  
  - Todas se basan en **dimensiones de silver** y **`fct_listing_snapshot`**, usando `dim_date` para cortes temporales cuando aplica.

---

### ✅ Tests en GOLD

En [`ab_nyc_dw/models/gold/schema.yml`](ab_nyc_dw\models\gold\schema.yml) puedes definir tests como:

- **`not_null`** y **`unique`** en claves técnicas.  
- **`relationships`** hacia dimensiones de silver cuando exista una FK lógica.  
- **Reglas de negocio** (tests personalizados) para asegurar rangos razonables (ej., valores positivos).

> Recomendación: gold suele tener tests más **ligeros** (sanidad y lógica), ya que la **calidad fuerte** se valida en **staging** y **silver**.


### ▶️ Ejecución (siguiendo lo que ya tenemos)

> **Flujo sugerido completo con _tests_ por capa:**

```bash
# 1) STAGING → build + tests
docker compose run --rm dbt sh -lc "dbt run --select staging && dbt test --select staging"

# 2) SNAPSHOTS (SCD-2)
docker compose run --rm dbt dbt snapshot

# 3) SILVER → build + tests
docker compose run --rm dbt sh -lc "dbt run --select silver && dbt test --select silver"

# 4) GOLD → build + tests
docker compose run --rm dbt sh -lc "dbt run --select gold && dbt test --select gold"

```
# ☁️ Orquestación con **Airflow** — Levantar el servicio y usar el DAG `ab_nyc_elt`

Esta sección explica, paso a paso, cómo **arrancar Airflow en contenedor**, registrar la **conexión a Postgres**, y **ejecutar** el DAG [`ab_nyc_elt`](dags/ab_nyc_elt.py) que orquesta: **extracción RAW → actualización de `latest.csv` → `dbt build` → chequeo en DWH**.

---

## 📁 Ubicación de archivos clave

- **DAG**: `dags/ab_nyc_elt.py`
- **Entorno**: `airflow.env` (variables de Airflow y admin)
- **Imagen**: [`airflow.Dockerfile`](airflow.Dockerfile)
- **Dependencias**: [`requirements_airflow.txt`](requirements_airflow.txt)
- **Bootstrap**: `airflow_init.sh` (migración DB y creación de usuario admin)

> Requisito de montajes (conceptual, en tu `docker-compose.yml`):  
> - `./dags` → `/opt/airflow/dags`  
> - **Raíz del repo** → `/opt/airflow/repo` (el DAG hace `cd /opt/airflow/repo`)  
> - `./logs_airflow` → `/opt/airflow/logs`  
> - `./data` → `/opt/airflow/repo/data` (para que el DAG vea `data/`)

---

## ▶️ Ciclo de arranque

### 1) Construir la imagen de Airflow
```bash
docker compose build airflow
```
### 2) Inicializar metadatos + usuario admin
```bash
docker compose run --rm airflow ./airflow_init.sh
```
### 2) levantar el servicio
```bash
docker compose up -d airflow
```
Interfaz web disponible (por defecto): http://localhost:8080

### Conexión `postgres_dw` (ejemplo genérico)

**En Airflow UI → _Admin → Connections_:**
- **Conn Id:** `postgres_dw`
- **Conn Type:** `Postgres`
- **Host:** `dw-host`            <!-- ej. el nombre del servicio en docker compose -->
- **Port:** `5432`
- **Schema:** `dw_database`       <!-- la base de datos donde dbt escribe/lee -->
- **Login:** `dw_user`
- **Password:** `dw_pass`

> Sugerencias:
> - Si usas Docker Compose y tu contenedor de Postgres se llama `postgres`, pon **Host = `postgres`**.
> - Presiona **Test** en la conexión para validar que Airflow llega a la BD.

**Vía CLI (URI en una línea):**
```bash
docker compose exec airflow \
  airflow connections add postgres_dw \
  --conn-uri 'postgresql+psycopg2://dw_user:dw_pass@dw-host:5432/dw_database'
```
## 🧩 ¿Qué hace el DAG [`ab_nyc_elt`](dags\ab_nyc_elt.py)?

### Grafo de tareas
```text
extract_raw  →  update_symlinks  →  dbt_build  →  gold_has_rows
```
🚀 Ejecutar el DAG
Opción A — Desde la UI

1. En DAGs, busca ab_nyc_elt, ponlo en Unpause (si aplica) y pulsa Trigger DAG.

2. Abre el Graph y monitorea los logs de cada tarea.

- Opción B — Desde CLI
```bash
# Disparar una corrida manual
docker compose exec airflow \
  airflow dags trigger ab_nyc_elt --run-id "manual_$(date -u +%Y%m%dT%H%M%SZ)"

# Ver el estado de las corridas
docker compose exec airflow airflow dags list-runs -d ab_nyc_elt

# Logs del servicio (scheduler/webserver) en vivo
docker compose logs -f airflow
```

# 📊 Consulta y visualización manual de **GOLD** (Postgres) con **DBeaver** y **Python**

> Esta sección va **después** de terminar la capa **GOLD** en dbt.  
> El DAG de Airflow aún no corre de punta a punta, puedes **conectarte manualmente al DWH** (Postgres) para explorar las vistas `public_gold.*` desde **DBeaver** y también con un pequeño **conector Python** para tablas y gráficos.

---

## 🧠 ¿Qué hace el conector? ([`db_conector.py`](notebook\db_conector.py))

- Lee variables de conexión desde **`.env`** (host, puerto, base, usuario, password).
- Crea un **SQLAlchemy Engine** y expone utilidades para usarlo.
- Funciones:
  - `get_db_engine()` → devuelve el `Engine` 
  - `get_db_session()` → devuelve una sesión ORM 
  - `get_db_connection()` → conexión cruda 

- Archivo para conectarse para visualizar los gráficos con análisis de preguntas de negocio:
- ['notebooks/db_conector.py'](notebook\db_conector.py)

- 📓 Notebook donde se encuentran los graficos con análisis de preguntas de negocio:
- ['notebooks/02_dashboard_ELT.ipynb'](notebook\02_dashboard_ELT.ipynb)

(abre una conexión con db_conector.py, ejecuta consultas a vistas public_gold.* y genera gráficos con pandas/matplotlib/etc)

- Asegúrate de que exista tu **`.env`** con las variables esperadas por el conector.

### 📄 Variables mínimas esperadas en `.env`
```env
# Postgres (ajusta a tu entorno real)
PGHOST=localhost
PGPORT=5432
PGDATABASE=ab_nyc_dw
PGUSER=tu_usuario
PGPASSWORD=tu_password
```



