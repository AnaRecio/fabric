import pytest


@pytest.fixture(scope="session")
def spark():
    pyspark_sql = pytest.importorskip("pyspark.sql")
    session = (
        pyspark_sql.SparkSession.builder.master("local[1]")
        .appName("andes_lib-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
