import dlt
from pyspark.sql.functions import col, expr, round
from pyspark.sql.types import DecimalType

products_rules = {
    "id_not_null" : "(id IS NOT NULL)",
    "created_at_not_null": "(created_at IS NOT NULL)",
    "price_not_null": "(price IS NOT NULL)",
    "price_positive": "(price > 0)",
    "category_not_null": "(category IS NOT NULL)",
    "category_not_empty": "(category != '')",
    "ean_not_null": "(ean IS NOT NULL)",
    "ean_not_empty": "(ean != '')",
    "quantity_not_null": "(quantity IS NOT NULL)",
    "quantity_positive": "(quantity > 0)",
    "rating_not_null": "(rating IS NOT NULL)",
    "rating_positive": "(rating > 0)",
    "title_not_null": "(title IS NOT NULL)",
    "title_not_empty": "(title != '')",
    "vendor_not_null": "(vendor IS NOT NULL)",
    "vendor_not_empty": "(vendor != '')"
}
products_quarantine_rules = "NOT({0})".format(" AND ".join(products_rules.values()))

@dlt.table(
    name = "products_quarantine",
    temporary=True,
    partition_cols=["is_quarantined"],
)
@dlt.expect_all(products_rules)
def products_quarantine():
    return (
        spark.readStream.table("stg_products").withColumn("is_quarantined", expr(products_quarantine_rules))
    )

@dlt.view
def valid_products_data():
    return spark.readStream.table("products_quarantine").filter("is_quarantined=false")

@dlt.table(
    name = "invalid_products_data",
)
def invalid_products_data():
    return spark.readStream.table("products_quarantine").filter("is_quarantined=true")

@dlt.view(
    name = "stg_products_transform"
)
def stg_products_transform():
    df = spark.readStream.table("valid_products_data")
    df = df.withColumnRenamed("id", "product_id")
    df = df.withColumn("price", round(col("price").cast(DecimalType(10,2)), 2))
    df = df.withColumn("rating", round(col("rating").cast(DecimalType(10,2)), 2))
    return df

dlt.create_streaming_table(
    name="transformed_products"
)

dlt.create_auto_cdc_flow(
    target = "transformed_products",
    source = "stg_products_transform",
    keys = ["product_id"],
    sequence_by = col("created_at"),
    apply_as_deletes = None,
    apply_as_truncates = None,
    except_column_list = ["is_quarantined"],
    stored_as_scd_type = 2
)





