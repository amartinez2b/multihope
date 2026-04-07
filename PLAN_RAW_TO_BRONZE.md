# Plan RAW → BRONZE (products, sales, shops)

## Estructura propuesta
- `src/raw_to_bronze/base_ingestion.py`: helper con la lógica genérica para leer tablas JDBC, contar registros y escribir en `data/bronze/<tabla>`; expone `ingest_table` reutilizable.
- `src/raw_to_bronze/products_ingestion.py`, `sales_ingestion.py`, `shops_ingestion.py`: entrypoints específicos por tabla que invocan `ingest_table` con el `app_name`, nombre de tabla y particiones necesarias.
- `tests/test_raw_to_bronze.py`: extender con casos parametrizados que validen que `ingest_table` aplica `partitionBy`, llama al driver JDBC con la tabla correcta y persiste en la ruta esperada (mockeando Spark como en el patrón actual).
- `data/bronze/products|sales|shops`: carpetas generadas automáticamente por los jobs; se mantienen fuera de git.
## Código PySpark
### `src/raw_to_bronze/base_ingestion.py`
```python
"""Utilidades comunes para la ingesta RAW -> BRONZE."""
import logging
from pathlib import Path
from typing import Sequence

from pyspark.sql import DataFrame

from src.utils.config_loader import load_db_config, get_jdbc_url
from src.utils.spark_session import create_spark_session

logger = logging.getLogger(__name__)
BRONZE_BASE = Path(__file__).parents[2] / "data" / "bronze"


def _read_raw_table(table: str, app_name: str) -> tuple[DataFrame, object]:
    db_config = load_db_config()
    spark = create_spark_session(app_name)

    logger.info("Conectando a %s/%s", db_config["host"], db_config["database"])
    df = (
        spark.read.format("jdbc")
        .option("url", get_jdbc_url(db_config))
        .option("dbtable", table)
        .option("user", db_config["user"])
        .option("password", db_config["password"])
        .option("driver", db_config["driver"])
        .load()
    )
    return df, spark


def ingest_table(
    *, table: str, app_name: str, output_subdir: str | None = None, partition_cols: Sequence[str] | None = None
) -> int:
    df, spark = _read_raw_table(table, app_name)
    try:
        record_count = df.count()
        target = BRONZE_BASE / (output_subdir or table)
        writer = df.write.mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)  # Mejora lectura en capas posteriores.
        writer.parquet(str(target))
        logger.info("%s registros escritos en %s", record_count, target)
        return record_count
    finally:
        spark.stop()
```

### `src/raw_to_bronze/products_ingestion.py`
```python
"""RAW -> BRONZE: products."""
from src.raw_to_bronze.base_ingestion import ingest_table


def ingest_products() -> int:
    return ingest_table(
        table="products",
        app_name="RAW_to_BRONZE_products",
        partition_cols=("category_id",),  # Pruning por categoría agiliza agregaciones GOLD.
    )


if __name__ == "__main__":
    ingest_products()
```

### `src/raw_to_bronze/sales_ingestion.py`
```python
"""RAW -> BRONZE: sales."""
from src.raw_to_bronze.base_ingestion import ingest_table


def ingest_sales() -> int:
    return ingest_table(
        table="sales",
        app_name="RAW_to_BRONZE_sales",
        partition_cols=("sale_date", "shop_id"),  # Fecha+tienda permite recargas incrementales.
    )


if __name__ == "__main__":
    ingest_sales()
```

### `src/raw_to_bronze/shops_ingestion.py`
```python
"""RAW -> BRONZE: shops."""
from src.raw_to_bronze.base_ingestion import ingest_table


def ingest_shops() -> int:
    return ingest_table(
        table="shops",
        app_name="RAW_to_BRONZE_shops",
    )


if __name__ == "__main__":
    ingest_shops()
```

## Pasos para ejecutar
1. Crear el entorno si no existe: `python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt`.
2. Copiar credenciales: `cp .env.example .env` y asignar `DB_USER`/`DB_PASSWORD` válidos (no se versionan).
3. Verificar la configuración JDBC en `config/database.yml` (driver MySQL y opciones SSL ya definidas).
4. Ejecutar cada job según la tabla necesaria:
   - `python -m src.raw_to_bronze.products_ingestion`
   - `python -m src.raw_to_bronze.sales_ingestion`
   - `python -m src.raw_to_bronze.shops_ingestion`
5. Confirmar los Parquet generados bajo `data/bronze/<tabla>` y, si aplica, revisar la partición esperada (`sale_date=YYYY-MM-DD/...`).
6. Correr `pytest tests/test_raw_to_bronze.py -k ingest_table` para validar que la capa base sigue escribiendo correctamente.
