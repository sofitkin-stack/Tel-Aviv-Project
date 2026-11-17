"""
Shared test setup: a local Spark session with Delta, and the helpers from Notebooks/00_setup.py.

00_setup.py is a Databricks notebook, so it cannot be imported as a module: it relies on the `spark`
global and one cell creates the catalog. We read the file, skip that cell and the self-test cell,
and run the remaining cells (imports, constants, functions) with a local `spark`.
"""
import os
from pathlib import Path

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

SETUP_NOTEBOOK = Path(__file__).resolve().parents[1] / "Notebooks" / "00_setup.py"
CELL_SEPARATOR = "# COMMAND ----------"


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    warehouse = tmp_path_factory.mktemp("warehouse")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("tlv-tests")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.sources.default", "delta")      # saveAsTable writes Delta, as on Databricks
        .config("spark.sql.shuffle.partitions", "2")
    )
    if os.environ.get("DELTA_JARS"):                       # offline: comma-separated local Delta jars
        session = builder.config("spark.jars", os.environ["DELTA_JARS"]).getOrCreate()
    else:                                                  # default: download the Delta package once
        session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def setup(spark):
    """The names defined by 00_setup (normalize_street, daily_compensation, upsert_rows, ...) as a dict."""
    namespace = {"spark": spark}
    for cell in SETUP_NOTEBOOK.read_text(encoding="utf-8").split(CELL_SEPARATOR):
        if "CREATE CATALOG" in cell or "self test" in cell or "%md" in cell:
            continue
        code = "\n".join(line for line in cell.splitlines() if not line.startswith("# MAGIC"))
        exec(code, namespace)
    return namespace
