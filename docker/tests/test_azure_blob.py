"""
Integration tests for Lance-Spark against Azure Blob Storage (Azurite emulator).

These tests exercise the same Lance-Spark operations as the local filesystem
tests but with data stored in Azure Blob Storage via the Azurite emulator
running as a subprocess inside the test container.
"""

import pytest
from pyspark.sql import SparkSession


CATALOG = "lance_az"


@pytest.fixture(scope="module")
def spark(azurite):
    """Create a SparkSession with a lance_az catalog backed by Azurite."""
    session = (
        SparkSession.builder
        .appName("LanceSparkAzureBlobTests")
        # Lance catalog pointing at Azure Blob Storage (Azurite)
        .config(
            f"spark.sql.catalog.{CATALOG}",
            "org.lance.spark.LanceNamespaceSparkCatalog",
        )
        .config(f"spark.sql.catalog.{CATALOG}.impl", "dir")
        .config(
            f"spark.sql.catalog.{CATALOG}.root",
            f"az://{azurite['container']}",
        )
        .config(
            f"spark.sql.catalog.{CATALOG}.storage.account_name",
            azurite["account_name"],
        )
        .config(
            f"spark.sql.catalog.{CATALOG}.storage.account_key",
            azurite["account_key"],
        )
        .config(
            f"spark.sql.catalog.{CATALOG}.storage.azure_storage_endpoint",
            azurite["endpoint"],
        )
        .config(
            f"spark.sql.catalog.{CATALOG}.storage.allow_http",
            "true",
        )
        .config(
            "spark.sql.extensions",
            "org.lance.spark.extensions.LanceSparkSessionExtensions",
        )
        .getOrCreate()
    )
    session.sql(f"SET spark.sql.defaultCatalog={CATALOG}")
    yield session
    session.stop()


@pytest.fixture(autouse=True)
def cleanup_tables(spark):
    """Drop test tables before and after each test."""
    spark.sql("DROP TABLE IF EXISTS default.test_table PURGE")
    yield
    spark.sql("DROP TABLE IF EXISTS default.test_table PURGE")


# =============================================================================
# DDL Tests
# =============================================================================

class TestAzureBlobDDL:
    """DDL operations against Azure Blob Storage."""

    def test_create_table(self, spark):
        """CREATE TABLE stores metadata in Azure Blob."""
        spark.sql("""
            CREATE TABLE default.test_table (
                id INT,
                name STRING,
                value DOUBLE
            )
        """)

        tables = spark.sql("SHOW TABLES IN default").collect()
        table_names = [row.tableName for row in tables]
        assert "test_table" in table_names

    def test_show_tables(self, spark):
        """SHOW TABLES lists tables stored in Azure Blob."""
        spark.sql("""
            CREATE TABLE default.test_table (
                id INT,
                name STRING
            )
        """)

        tables = spark.sql("SHOW TABLES IN default").collect()
        assert len(tables) >= 1
        table_names = [row.tableName for row in tables]
        assert "test_table" in table_names

    def test_drop_table(self, spark):
        """DROP TABLE removes the table from Azure Blob."""
        spark.sql("""
            CREATE TABLE default.test_table (
                id INT,
                name STRING
            )
        """)

        spark.sql("DROP TABLE IF EXISTS default.test_table PURGE")

        tables = spark.sql("SHOW TABLES IN default").collect()
        table_names = [row.tableName for row in tables]
        assert "test_table" not in table_names


# =============================================================================
# DML Tests
# =============================================================================

class TestAzureBlobDML:
    """DML operations against Azure Blob Storage."""

    def test_insert_into_values(self, spark):
        """INSERT INTO with VALUES writes data to Azure Blob."""
        spark.sql("""
            CREATE TABLE default.test_table (
                id INT,
                name STRING,
                value DOUBLE
            )
        """)

        spark.sql("""
            INSERT INTO default.test_table VALUES
            (1, 'Alice', 10.5),
            (2, 'Bob', 20.3),
            (3, 'Charlie', 30.1)
        """)

        result = spark.table("default.test_table").collect()
        assert len(result) == 3

        ids = sorted([row.id for row in result])
        assert ids == [1, 2, 3]

    def test_insert_dataframe_api(self, spark):
        """DataFrame writeTo().append() writes data to Azure Blob."""
        spark.sql("""
            CREATE TABLE default.test_table (
                id INT,
                name STRING,
                value DOUBLE
            )
        """)

        data = [(1, "Alice", 10.5), (2, "Bob", 20.3)]
        df = spark.createDataFrame(data, ["id", "name", "value"])
        df.writeTo("default.test_table").append()

        count = spark.table("default.test_table").count()
        assert count == 2

    def test_select_with_where(self, spark):
        """SELECT with WHERE reads filtered data from Azure Blob."""
        spark.sql("""
            CREATE TABLE default.test_table (
                id INT,
                name STRING,
                value DOUBLE
            )
        """)

        data = [
            (1, "Alice", 10.5),
            (2, "Bob", 20.3),
            (3, "Charlie", 30.1),
        ]
        df = spark.createDataFrame(data, ["id", "name", "value"])
        df.writeTo("default.test_table").append()

        result = spark.sql("""
            SELECT * FROM default.test_table WHERE id >= 2
        """).collect()

        assert len(result) == 2
        names = sorted([row.name for row in result])
        assert names == ["Bob", "Charlie"]

    def test_count(self, spark):
        """COUNT(*) returns correct row count from Azure Blob."""
        spark.sql("""
            CREATE TABLE default.test_table (
                id INT,
                name STRING
            )
        """)

        data = [(i, f"Name{i}") for i in range(50)]
        df = spark.createDataFrame(data, ["id", "name"])
        df.writeTo("default.test_table").append()

        count = spark.sql(
            "SELECT COUNT(*) as cnt FROM default.test_table"
        ).collect()[0].cnt
        assert count == 50


# =============================================================================
# Lifecycle Test
# =============================================================================

class TestAzureBlobLifecycle:
    """End-to-end lifecycle: create -> write -> read -> drop -> verify gone."""

    def test_full_lifecycle(self, spark):
        spark.sql("""
            CREATE TABLE default.test_table (
                id INT,
                name STRING,
                value DOUBLE
            )
        """)

        # Write
        data = [(1, "Alice", 10.5), (2, "Bob", 20.3), (3, "Charlie", 30.1)]
        df = spark.createDataFrame(data, ["id", "name", "value"])
        df.writeTo("default.test_table").append()

        # Read
        result = spark.table("default.test_table").orderBy("id").collect()
        assert len(result) == 3
        assert result[0].name == "Alice"
        assert result[2].name == "Charlie"

        # Drop
        spark.sql("DROP TABLE IF EXISTS default.test_table PURGE")

        # Verify gone
        tables = spark.sql("SHOW TABLES IN default").collect()
        table_names = [row.tableName for row in tables]
        assert "test_table" not in table_names
