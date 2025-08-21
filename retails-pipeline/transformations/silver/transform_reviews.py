import dlt
from pyspark.sql.functions import col, expr, round, to_date
from pyspark.sql.types import DecimalType

reviews_rules = {
    "id_not_null" : "(id IS NOT NULL)",
    "created_at_not_null": "(created_at IS NOT NULL)",
    "reviewer_not_null": "(reviewer IS NOT NULL)",
    "revierer_not_empty": "(reviewer != '')",
    "product_id_not_null": "(product_id IS NOT NULL)",
    "product_id_positive": "(product_id > 0)",
    "rating_not_null": "(rating IS NOT NULL)",
    "rating_positive": "(rating > 0)",
    "body_not_null": "(body IS NOT NULL)",
    "body_not_empty": "(body != '')"
}
reviews_quarantine_rules = "NOT({0})".format(" AND ".join(reviews_rules.values()))

@dlt.table(
    name = "reviews_quarantine",
    temporary=True,
    partition_cols=["is_quarantined"],
)
@dlt.expect_all(reviews_rules)
def reviews_quarantine():
    return (
        spark.readStream.table("stg_reviews").withColumn("is_quarantined", expr(reviews_quarantine_rules))
    )

@dlt.view
def valid_reviews_data():
    return spark.readStream.table("reviews_quarantine").filter("is_quarantined=false")

@dlt.table(
    name = "invalid_reviews_data",
)
def invalid_reviews_data():
    return spark.readStream.table("reviews_quarantine").filter("is_quarantined=true")

@dlt.view(
    name = "stg_reviews_transform"
)
def stg_reviews_transform():
    df = spark.readStream.table("valid_reviews_data")
    df = df.withColumnRenamed("id", "review_id")
    return df

dlt.create_streaming_table(
    name="transformed_reviews"
)

dlt.create_auto_cdc_flow(
    target = "transformed_reviews",
    source = "stg_reviews_transform",
    keys = ["review_id"],
    sequence_by = col("created_at"),
    apply_as_deletes = None,
    apply_as_truncates = None,
    except_column_list = ["is_quarantined"],
    stored_as_scd_type = 2
)





