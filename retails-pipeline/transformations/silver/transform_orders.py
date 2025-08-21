import dlt
from pyspark.sql.functions import col, expr, round, to_date, when
from pyspark.sql.types import DecimalType

orders_rules = {
    "id_not_null" : "(id IS NOT NULL)",
    "created_at_not_null": "(created_at IS NOT NULL)",
    "user_id_not_null": "(user_id IS NOT NULL)",
    "user_id_positive": "(user_id > 0)",
    "product_id_not_null": "(product_id IS NOT NULL)",
    "product_id_positive": "(product_id > 0)",
    "quantity_not_null": "(quantity IS NOT NULL)",
    "quantity_positive": "(quantity > 0)",
    "subtotal_not_null": "(subtotal IS NOT NULL)",
    "subtotal_positive": "(subtotal > 0)",
    "tax_not_null": "(tax IS NOT NULL)",
    "tax_positive": "(tax > 0)",
    "total_not_null": "(total IS NOT NULL)",
    "total_positive": "(total > 0)"
}
orders_quarantine_rules = "NOT({0})".format(" AND ".join(orders_rules.values()))

@dlt.table(
    name = "orders_quarantine",
    temporary=True,
    partition_cols=["is_quarantined"],
)
@dlt.expect_all(orders_rules)
def orders_quarantine():
    return (
        spark.readStream.table("stg_orders").withColumn("is_quarantined", expr(orders_quarantine_rules))
    )

@dlt.view
def valid_orders_data():
    return spark.readStream.table("orders_quarantine").filter("is_quarantined=false")

@dlt.table(
    name = "invalid_orders_data",
)
def invalid_orders_data():
    return spark.readStream.table("orders_quarantine").filter("is_quarantined=true")

@dlt.view(
    name = "stg_orders_transform"
)
def stg_orders_transform():
    df = spark.readStream.table("valid_orders_data")
    df = df.withColumnRenamed("id", "order_id")
    df = df.withColumn("discount", when(col("discount").isNull(), 0).otherwise(col("discount")))
    df = df.withColumn("discount", round(col("discount").cast(DecimalType(10,2)), 2))
    df = df.withColumn("subtotal", round(col("subtotal").cast(DecimalType(10,2)), 2))
    df = df.withColumn("tax", round(col("tax").cast(DecimalType(10,2)), 2))
    df = df.withColumn("total", round(col("total").cast(DecimalType(10,2)), 2))
    return df

orders_rules_after_transform = {
    "discount_not_null" : "(discount IS NOT NULL)",
    "discount_gt_0" : "(discount >= 0)"
}

dlt.create_streaming_table(
    name="transformed_orders",
    expect_all = orders_rules_after_transform
)

dlt.create_auto_cdc_flow(
    target = "transformed_orders",
    source = "stg_orders_transform",
    keys = ["order_id"],
    sequence_by = col("created_at"),
    apply_as_deletes = None,
    apply_as_truncates = None,
    except_column_list = ["is_quarantined"],
    stored_as_scd_type = 2
)





