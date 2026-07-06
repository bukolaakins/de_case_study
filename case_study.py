"""
Data Engineering Take-Home Assessment
======================================

Standalone Python implementation of the notebook solution.

1. Load raw source data
2. Data quality review (nulls, duplicates, format checks)
3. Standardise the data and assign data types  -> "store" tables
4. Data quality validation (primary keys, foreign keys, ranges, reconciliation)
5. Publish layer business transformations       -> "publish" tables
6. Analysis questions

Author: Bukola Akinsola
"""

from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DoubleType, BooleanType

# Folder containing the input CSV files
DATA_DIR = "./data"



DATE_PATTERN = r"^\d{4}-\d{2}-\d{2}$"
DECIMAL_PATTERN = r"^(\d+(\.\d+)?|\.\d+)$"


# ---------------------------------------------------------------------------
# Helper functions used during the data quality review
# ---------------------------------------------------------------------------

def read_csv(spark, file_name):
    """Read a source CSV, keeping every column as a string for now."""
    return (
        spark.read
        .format("csv")
        .option("header", True)         # first row is the column names
        .option("quote", '"')
        .option("escape", '"')
        .option("inferSchema", False)   # cast to proper types later
        .csv(f"{DATA_DIR}/{file_name}")
    )


def check_nulls(df):
    """Return a one-row DataFrame showing how many NULLs are in each column."""
    return df.select([
        F.count(F.when(F.col(c).isNull(), c)).alias(c)
        for c in df.columns
    ])


def check_duplicate_ids(df, id_columns):
    """Return rows where id_columns is repeated more than once."""
    return (
        df.groupBy(id_columns)
        .count()
        .filter(F.col("count") > 1)
    )


def check_format(df, column, pattern):
    """Return distinct values in column that don't match the expected pattern."""
    return (
        df.filter(
            F.col(column).isNotNull() &
            ~F.col(column).rlike(pattern)
        )
        .select(column)
        .distinct()
    )


def validate_decimal_columns(df, table_name, columns):
    """Print any values in columns that are not valid decimal numbers."""
    print(f"\nValidating decimal columns in {table_name}")
    for column in columns:
        print(f"Checking {column}...")
        check_format(df, column, DECIMAL_PATTERN).show(truncate=False)


# ---------------------------------------------------------------------------
# Helper functions used during data quality validation (on the store tables)
# ---------------------------------------------------------------------------

def validate_primary_key(df, table_name, key_columns):
    """Check that key_columns uniquely identifies each row and has no NULLs."""
    print(f"\nPrimary Key Validation: {table_name}")

    duplicates = df.groupBy(*key_columns).count().filter(F.col("count") > 1)
    duplicate_count = duplicates.count()
    if duplicate_count == 0:
        print("No duplicate primary keys found.")
    else:
        print(f"{duplicate_count} duplicate primary key(s) found.")
        duplicates.show(truncate=False)

    null_condition = reduce(lambda a, b: a | b, [F.col(c).isNull() for c in key_columns])
    null_rows = df.filter(null_condition)
    null_count = null_rows.count()
    if null_count == 0:
        print("No NULL primary keys found.")
    else:
        print(f"{null_count} NULL primary key(s) found.")
        null_rows.show(truncate=False)


def validate_foreign_key(child_df, parent_df, child_key, parent_key, relationship_name):
    """Check that every child_key value exists in parent_df[parent_key]."""
    print(f"\nReferential Integrity: {relationship_name}")

    orphan_rows = (
        child_df.alias("child")
        .join(
            parent_df.select(parent_key).distinct().alias("parent"),
            F.col(f"child.{child_key}") == F.col(f"parent.{parent_key}"),
            "left_anti"
        )
    )
    orphan_count = orphan_rows.count()
    if orphan_count == 0:
        print("No referential integrity issues found.")
    else:
        print(f"{orphan_count} orphan record(s) found.")
        orphan_rows.show(truncate=False)


def validate_numeric_range(df, table_name, column_name, minimum_value=0, inclusive=True):
    """
    Check that column_name stays within the expected business range.
    inclusive=True  -> value >= minimum_value
    inclusive=False -> value >  minimum_value
    """
    print(f"\nNumeric Range Validation: {table_name}.{column_name}")

    if inclusive:
        invalid_rows = df.filter(F.col(column_name) < minimum_value)
    else:
        invalid_rows = df.filter(F.col(column_name) <= minimum_value)

    invalid_count = invalid_rows.count()
    if invalid_count == 0:
        print("Validation passed.")
    else:
        print(f"{invalid_count} invalid record(s) found.")
        invalid_rows.show(truncate=False)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def main():
    spark = SparkSession.builder.appName("TakeHomeAssessment").getOrCreate()

    # ------------------------------------------------------------------
    # 1. Load raw data
    # ------------------------------------------------------------------
    raw_product = read_csv(spark, "products.csv")
    raw_sales_order_header = read_csv(spark, "sales_order_header.csv")
    raw_sales_order_detail = read_csv(spark, "sales_order_detail.csv")

    print(f"raw_product has {raw_product.count()} rows")
    print(f"raw_sales_order_header has {raw_sales_order_header.count()} rows")
    print(f"raw_sales_order_detail has {raw_sales_order_detail.count()} rows")

    # ------------------------------------------------------------------
    # 2. Data quality review (read-only checks on the raw data)
    # ------------------------------------------------------------------
    check_nulls(raw_product).show()
    check_nulls(raw_sales_order_header).show()
    check_nulls(raw_sales_order_detail).show()

    duplicate_product_ids = check_duplicate_ids(raw_product, ["ProductID"])
    print(f"Duplicate ProductID groups found: {duplicate_product_ids.count()}")

    check_format(raw_sales_order_header, "OrderDate", DATE_PATTERN).show(truncate=False)
    check_format(raw_sales_order_header, "ShipDate", DATE_PATTERN).show(truncate=False)

    validate_decimal_columns(raw_product, "Product", ["StandardCost", "ListPrice", "Weight"])
    validate_decimal_columns(raw_sales_order_header, "SalesOrderHeader", ["Freight"])
    validate_decimal_columns(raw_sales_order_detail, "SalesOrderDetail", ["UnitPrice", "UnitPriceDiscount"])

    # ------------------------------------------------------------------
    # 3. Standardise the data and assign types -> store tables
    # ------------------------------------------------------------------

    # Remove leading/trailing whitespace from the unit measure codes
    raw_product = (
        raw_product
        .withColumn("SizeUnitMeasureCode", F.trim("SizeUnitMeasureCode"))
        .withColumn("WeightUnitMeasureCode", F.trim("WeightUnitMeasureCode"))
    )

    # Some ProductID values appear more than once. Keep the most complete
    # record for each ProductID (the one with category/sub-category filled in).
    window = (
        Window
        .partitionBy("ProductID")
        .orderBy(
            F.col("ProductCategoryName").isNull(),
            F.col("ProductSubCategoryName").isNull()
        )
    )
    store_product = (
        raw_product
        .withColumn("row_num", F.row_number().over(window))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    # A few OrderDate/ShipDate values are in an incomplete yyyy-MM format.
    # The day cannot be reliably guessed, so these are set to NULL instead.
    store_sales_order_header = (
        raw_sales_order_header
        .withColumn("OrderDate", F.when(F.col("OrderDate").rlike(DATE_PATTERN), F.col("OrderDate")))
        .withColumn("ShipDate", F.when(F.col("ShipDate").rlike(DATE_PATTERN), F.col("ShipDate")))
    )

    # Assign proper data types now that the values have been cleaned up
    store_product = (
        store_product
        .withColumn("ProductID", F.col("ProductID").cast(IntegerType()))
        .withColumn("MakeFlag", F.col("MakeFlag").cast(BooleanType()))
        .withColumn("SafetyStockLevel", F.col("SafetyStockLevel").cast(IntegerType()))
        .withColumn("ReorderPoint", F.col("ReorderPoint").cast(IntegerType()))
        .withColumn("StandardCost", F.col("StandardCost").cast(DoubleType()))
        .withColumn("ListPrice", F.col("ListPrice").cast(DoubleType()))
        .withColumn("Weight", F.col("Weight").cast(DoubleType()))
    )

    store_sales_order_header = (
        store_sales_order_header
        .withColumn("SalesOrderID", F.col("SalesOrderID").cast(IntegerType()))
        .withColumn("OrderDate", F.to_date("OrderDate"))
        .withColumn("ShipDate", F.to_date("ShipDate"))
        .withColumn("OnlineOrderFlag", F.col("OnlineOrderFlag").cast(BooleanType()))
        .withColumn("CustomerID", F.col("CustomerID").cast(IntegerType()))
        .withColumn("SalesPersonID", F.col("SalesPersonID").cast(IntegerType()))
        .withColumn("Freight", F.col("Freight").cast(DoubleType()))
    )

    store_sales_order_detail = (
        raw_sales_order_detail
        .withColumn("SalesOrderID", F.col("SalesOrderID").cast(IntegerType()))
        .withColumn("SalesOrderDetailID", F.col("SalesOrderDetailID").cast(IntegerType()))
        .withColumn("OrderQty", F.col("OrderQty").cast(IntegerType()))
        .withColumn("ProductID", F.col("ProductID").cast(IntegerType()))
        .withColumn("UnitPrice", F.col("UnitPrice").cast(DoubleType()))
        .withColumn("UnitPriceDiscount", F.col("UnitPriceDiscount").cast(DoubleType()))
    )

    # ------------------------------------------------------------------
    # 4. Data quality validation on the store tables
    # ------------------------------------------------------------------
    validate_primary_key(store_product, "Product", ["ProductID"])
    validate_primary_key(store_sales_order_header, "SalesOrderHeader", ["SalesOrderID"])
    validate_primary_key(store_sales_order_detail, "SalesOrderDetail", ["SalesOrderDetailID"])

    validate_foreign_key(
        store_sales_order_detail, store_sales_order_header,
        "SalesOrderID", "SalesOrderID", "SalesOrderDetail -> SalesOrderHeader"
    )
    validate_foreign_key(
        store_sales_order_detail, store_product,
        "ProductID", "ProductID", "SalesOrderDetail -> Product"
    )

    # ShipDate should never be before OrderDate
    store_sales_order_header.filter(
        F.col("ShipDate") < F.col("OrderDate")
    ).show(truncate=False)

    validate_numeric_range(store_sales_order_detail, "SalesOrderDetail", "OrderQty", minimum_value=0, inclusive=False)
    validate_numeric_range(store_sales_order_detail, "SalesOrderDetail", "UnitPrice")
    validate_numeric_range(store_sales_order_detail, "SalesOrderDetail", "UnitPriceDiscount")
    validate_numeric_range(store_sales_order_header, "SalesOrderHeader", "Freight")

    # Record count reconciliation - make sure standardising didn't drop/add rows unexpectedly
    print("\nRecord Count Reconciliation")
    row_counts = [
        ("Product", raw_product.count(), store_product.count()),
        ("SalesOrderHeader", raw_sales_order_header.count(), store_sales_order_header.count()),
        ("SalesOrderDetail", raw_sales_order_detail.count(), store_sales_order_detail.count()),
    ]
    for table, raw_count, store_count in row_counts:
        print(f"{table:<20}{raw_count:>10}{store_count:>10}{store_count - raw_count:>15}")

    # ------------------------------------------------------------------
    # 5. Publish layer - business transformations
    # ------------------------------------------------------------------

    # Product master: default missing colour to "N/A" and backfill missing
    # ProductCategoryName from ProductSubCategoryName using the business rules
    publish_product = (
        store_product
        .fillna({"Color": "N/A"})
        .withColumn(
            "ProductCategoryName",
            F.when(F.col("ProductCategoryName").isNotNull(), F.col("ProductCategoryName"))
            .when(
                F.col("ProductSubCategoryName").isin("Gloves", "Shorts", "Socks", "Tights", "Vests"),
                F.lit("Clothing")
            )
            .when(
                F.col("ProductSubCategoryName").isin(
                    "Locks", "Lights", "Headsets", "Helmets", "Pedals", "Pumps"
                ),
                F.lit("Accessories")
            )
            .when(
                (F.col("ProductSubCategoryName").contains("Frames")) |
                (F.col("ProductSubCategoryName").isin("Wheels", "Saddles")),
                F.lit("Components")
            )
            .otherwise(F.col("ProductCategoryName"))
        )
    )

    # Sales orders: join detail + header, then add the required calculated columns
    orders_df = (
        store_sales_order_detail.alias("detail")
        .join(store_sales_order_header.alias("header"), on="SalesOrderID", how="inner")
    )

    # Business days between OrderDate and ShipDate
    orders_df = orders_df.withColumn(
        "LeadTimeInBusinessDays",
        F.size(
            F.filter(
                F.sequence(F.col("OrderDate"), F.col("ShipDate")),
                lambda d: F.dayofweek(d).between(2, 6)
            )
        ) - 1
    )

    # Extended price per order line
    orders_df = orders_df.withColumn(
        "TotalLineExtendedPrice",
        F.col("OrderQty") * (F.col("UnitPrice") - F.col("UnitPriceDiscount"))
    )

    orders_df = orders_df.withColumnRenamed("Freight", "TotalOrderFreight")

    publish_orders = orders_df.select(
        *store_sales_order_detail.columns,
        "OrderDate",
        "ShipDate",
        "OnlineOrderFlag",
        "AccountNumber",
        "CustomerID",
        "SalesPersonID",
        "TotalOrderFreight",
        "LeadTimeInBusinessDays",
        "TotalLineExtendedPrice",
    )

    publish_orders.show(10, truncate=False)

    # ------------------------------------------------------------------
    # 6. Analysis questions
    # ------------------------------------------------------------------

    # Q1: Which colour generated the highest revenue each year?
    color_revenue_df = (
        publish_orders
        .filter(F.col("OrderDate").isNotNull())
        .join(publish_product.select("ProductID", "Color"), on="ProductID", how="left")
        .withColumn("Year", F.year("OrderDate"))
    )

    color_revenue_agg = (
        color_revenue_df
        .groupBy("Year", "Color")
        .agg(F.sum("TotalLineExtendedPrice").alias("TotalRevenue"))
    )

    year_window = Window.partitionBy("Year").orderBy(F.desc("TotalRevenue"))

    top_color_each_year = (
        color_revenue_agg
        .withColumn("rank", F.row_number().over(year_window))
        .filter(F.col("rank") == 1)
        .drop("rank")
    )

    print("\nQ1 -- Highest revenue colour per year:")
    top_color_each_year.show()

    # Q2: What is the average LeadTimeInBusinessDays by ProductCategoryName?
    lead_time_df = (
        publish_orders
        .join(publish_product.select("ProductID", "ProductCategoryName"), on="ProductID", how="left")
    )

    avg_lead_time = (
        lead_time_df
        .groupBy("ProductCategoryName")
        .agg(F.avg("LeadTimeInBusinessDays").alias("AvgLeadTimeInBusinessDays"))
        .filter(F.col("ProductCategoryName").isNotNull())
        .orderBy(F.desc("AvgLeadTimeInBusinessDays"))
    )

    print("\nQ2 -- Average LeadTimeInBusinessDays by ProductCategoryName:")
    avg_lead_time.show()


    spark.stop()


if __name__ == "__main__":
    main()
