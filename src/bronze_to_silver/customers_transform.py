"""BRONZE -> SILVER customers using databricks-labs-dqx 0.13.0."""
import logging
import sys
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

sys.path.insert(0, str(Path(__file__).parents[2]))

from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.engine import DQEngineCore
from databricks.labs.dqx.rule import DQRowRule

from src.utils.spark_session import create_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parents[2]
BRONZE_INPUT = PROJECT_ROOT / "data" / "bronze" / "customers"
SILVER_OUTPUT = PROJECT_ROOT / "data" / "silver" / "customers"
QUARANTINE_OUTPUT = PROJECT_ROOT / "data" / "quarantine" / "customers"
REQUIRED_COLUMNS = {"customer_id", "nombre", "email", "identificacion", "estado"}


class _LocalWorkspaceConfig:
    def __init__(self) -> None:
        self._product_info = ("dqx", "local")

    def copy(self) -> "_LocalWorkspaceConfig":
        return _LocalWorkspaceConfig()

    def with_user_agent_extra(self, key: str, value: str) -> "_LocalWorkspaceConfig":
        return self


class _LocalClusters:
    @staticmethod
    def select_spark_version() -> str:
        return "local"


class _LocalWorkspaceClient:
    def __init__(self, config: _LocalWorkspaceConfig | None = None) -> None:
        self.config = config or _LocalWorkspaceConfig()
        self.clusters = _LocalClusters()


def _quality_checks() -> list[DQRowRule]:
    return [
        DQRowRule(
            name="customer_id_not_null",
            criticality="error",
            check_func=check_funcs.is_not_null,
            column="customer_id",
        ),
        DQRowRule(
            name="nombre_not_null_or_empty",
            criticality="error",
            check_func=check_funcs.is_not_null_and_not_empty,
            column="nombre",
        ),
        DQRowRule(
            name="email_not_null_or_empty",
            criticality="error",
            check_func=check_funcs.is_not_null_and_not_empty,
            column="email",
        ),
        DQRowRule(
            name="identificacion_not_empty",
            criticality="warn",
            check_func=check_funcs.is_not_null_and_not_empty,
            column="identificacion",
        ),
        DQRowRule(
            name="estado_not_null",
            criticality="warn",
            check_func=check_funcs.is_not_null,
            column="estado",
        ),
    ]


def _rename_identifier(df: DataFrame) -> DataFrame:
    if "id_cliente" in df.columns and "customer_id" not in df.columns:
        return df.withColumnRenamed("id_cliente", "customer_id")
    return df


def _standardize_strings(df: DataFrame) -> DataFrame:
    trimmed_cols = [
        F.trim(F.col(col_name)).alias(col_name) if dtype == "string" else F.col(col_name)
        for col_name, dtype in df.dtypes
    ]
    return df.select(trimmed_cols)


def _validate_columns(df: DataFrame) -> None:
    missing = REQUIRED_COLUMNS.difference(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in Bronze customers dataset: {sorted(missing)}")


def _prepare_customers(df: DataFrame) -> DataFrame:
    renamed_df = _rename_identifier(df)
    cleaned_df = _standardize_strings(renamed_df).dropDuplicates()
    _validate_columns(cleaned_df)
    return (
        cleaned_df
        .withColumn(
            "email",
            F.when(F.col("email").isNotNull(), F.lower(F.col("email"))).otherwise(F.col("email"))
        )
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_layer", F.lit("bronze"))
    )


def transform_customers() -> int:
    logger.info("=== BRONZE -> SILVER | customers ===")
    spark = create_spark_session("BRONZE_to_SILVER_customers")
    try:
        logger.info("Reading Bronze data from %s", BRONZE_INPUT)
        bronze_df = spark.read.parquet(str(BRONZE_INPUT))
        enriched_df = _prepare_customers(bronze_df)

        logger.info("Running DQX checks")
        engine = DQEngineCore(_LocalWorkspaceClient(), spark)
        valid_df, quarantine_df = engine.apply_checks_and_split(enriched_df, _quality_checks())

        valid_count = valid_df.count()
        quarantine_count = quarantine_df.count()
        logger.info("Quality results — valid: %s, quarantined: %s", valid_count, quarantine_count)

        logger.info("Writing Silver output to %s", SILVER_OUTPUT)
        valid_df.write.mode("overwrite").parquet(str(SILVER_OUTPUT))

        if quarantine_count:
            logger.warning("%s rows moved to quarantine", quarantine_count)
            quarantine_df.write.mode("overwrite").option("compression", "snappy").parquet(
                str(QUARANTINE_OUTPUT)
            )
        else:
            logger.info("No quarantined rows produced")

        logger.info("Silver layer complete")
        return valid_count
    finally:
        spark.stop()


if __name__ == "__main__":
    transform_customers()
