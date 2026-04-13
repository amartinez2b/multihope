# Multihope — Pipeline Medallion con PySpark

Pipeline de datos por capas (RAW → BRONZE → SILVER → GOLD) usando PySpark y arquitectura Medallion.

## Estructura del proyecto

```
multihope/
├── config/
│   ├── database.yml          # Host, puerto y base de datos MySQL
│   └── spark_config.yml      # Configuracion de SparkSession
├── src/
│   ├── utils/
│   │   ├── config_loader.py  # Carga YAML + .env
│   │   └── spark_session.py  # Fabrica de SparkSession
│   ├── raw_to_bronze/
│   │   └── customers_ingestion.py
│   ├── bronze_to_silver/
│   │   └── customers_transform.py
│   └── silver_to_gold/
│       └── customers_aggregation.py
├── catalog/
│   └── comercial/
│       ├── catalog.md            # Catalogo de datos (legible)
│       └── catalog.json          # Catalogo de datos (machine-readable para IA)
├── notebooks/
│   ├── 00_precheck.ipynb
│   ├── 01_raw_to_bronze_customers.ipynb
│   ├── 02_bronze_to_silver_customers.ipynb
│   └── 03_silver_to_gold_customers.ipynb
├── tests/
│   ├── test_raw_to_bronze.py
│   ├── test_bronze_to_silver.py
│   └── test_silver_to_gold.py
├── data/                     # Generado en ejecucion (gitignored)
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── .env                      # Credenciales (gitignored)
├── .env.example              # Plantilla de credenciales
└── requirements.txt
```

## Setup

```bash
# 1. Crear entorno virtual
python -m venv .venv
source .venv/bin/activate

# 2. Instalar dependencias
pip install -r requirements.txt

# 3. Configurar credenciales
cp .env.example .env
# Editar .env con usuario y password reales
# usuario: curso
# clave: Pídela a tu instructor
```

## Ejecucion

### Por scripts

```bash
# RAW -> BRONZE
python -m src.raw_to_bronze.customers_ingestion

# BRONZE -> SILVER
python -m src.bronze_to_silver.customers_transform

# SILVER -> GOLD
python -m src.silver_to_gold.customers_aggregation
```

### Por notebooks

Ejecutar en orden dentro de notebooks/:
1. 01_raw_to_bronze_customers.ipynb
2. 02_bronze_to_silver_customers.ipynb
3. 03_silver_to_gold_customers.ipynb

## Catalogo de datos

El directorio `catalog/` contiene la documentacion del producto de datos **Comercial**, que integra las tablas `customers`, `products`, `sales` y `shops` de la base de datos `fake`.

| Archivo | Descripcion |
|---|---|
| `catalog/comercial/catalog.md` | Documentacion completa: tablas, columnas, relaciones, ejemplos, consultas SQL de referencia y glosario de negocio |
| `catalog/comercial/catalog.json` | Version machine-readable optimizada para que una IA genere consultas SQL: tipos, FK, valores validos, estadisticas y patrones SQL |

### Tablas documentadas

| Tabla | Descripcion | Filas |
|---|---|---|
| `customers` | Clientes registrados | 10 |
| `products` | Catalogo de productos de limpieza | 5 |
| `sales` | Transacciones de venta (tabla de hechos) | 100 |
| `shops` | Sucursales en Ecuador (Quito, Guayaquil, Cuenca) | 3 |

## Tests

```bash
pytest tests/ -v
```

## Configuracion de GitHub para contribuidores

### 1. Clonar el repositorio

```bash
git clone https://github.com/amartinez2b/multihope.git
cd multihope
```

### 2. Generar un Personal Access Token (PAT)

1. Inicia sesion en GitHub con tu cuenta
2. Ve a **Settings → Developer settings → Personal access tokens → Tokens (classic)**
3. Haz clic en **Generate new token**
4. Asignale un nombre descriptivo (ej. `multihope-local`) y activa el scope `repo`
5. Copia el token generado (solo se muestra una vez)

### 3. Configurar tu identidad git en el repo

```bash
# Identidad local (solo aplica a este repo)
git config user.name "Tu Nombre"
git config user.email "tucorreo@bigdataybi.com"
```

Si tienes otra identidad global para otros proyectos, la config local la sobreescribe unicamente dentro de este directorio.

### 4. Incluir tu usuario en el remote URL

Esto permite que macOS/Linux guarde el token asociado a tu usuario y no lo confunda con otras cuentas:

```bash
git remote set-url origin https://<tu-usuario-github>@github.com/amartinez2b/multihope.git
```

### 5. Hacer push

```bash
git push
```

La primera vez te pedira contrasena: ingresa el **Personal Access Token** del paso 2. El sistema operativo lo guardara en el gestor de credenciales y no te lo volvera a solicitar.

> **Nota macOS:** si persiste un error 403 por credenciales viejas en el Keychain, ejecuta primero:
> ```bash
> git credential-osxkeychain erase
> host=github.com
> protocol=https
> [Enter dos veces]
> ```

## Conexion MySQL

| Parametro | Valor               |
|-----------|---------------------|
| Host      | www.bigdataybi.com  |
| Port      | 3306                |
| Database  | fake                |
| Driver    | MySQL Connector/J   |


Cambio Prueba