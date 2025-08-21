import dlt
from pyspark.sql.functions import col, expr, round, to_date
from pyspark.sql.types import DecimalType

users_rules = {
    "id_not_null" : "(id IS NOT NULL)",
    "created_at_not_null": "(created_at IS NOT NULL)",
    "name_not_null": "(name IS NOT NULL)",
    "name_not_empty": "(name != '')",
    "email_not_null": "(email IS NOT NULL)",
    "email_not_empty": "(email != '')",
    "address_not_null": "(address IS NOT NULL)",
    "address_not_empty": "(address != '')",
    "city_not_null": "(city IS NOT NULL)",
    "city_not_empty": "(city != '')",
    "state_not_null": "(state IS NOT NULL)",
    "state_not_empty": "(state != '')",
    "zip_not_null": "(zip IS NOT NULL)",
    "zip_not_empty": "(zip != '')",
    "birth_date_not_null": "(birth_date IS NOT NULL)",
    "birth_date_not_empty": "(birth_date != '')",
    "latitude_not_null": "(latitude IS NOT NULL)",
    "longitude_not_null": "(longitude IS NOT NULL)",
    "source_not_null": "(source IS NOT NULL)",
    "source_not_empty": "(source != '')"
}
users_quarantine_rules = "NOT({0})".format(" AND ".join(users_rules.values()))

@dlt.table(
    name = "users_quarantine",
    temporary=True,
    partition_cols=["is_quarantined"],
)
@dlt.expect_all(users_rules)
def users_quarantine():
    return (
        spark.readStream.table("stg_users").withColumn("is_quarantined", expr(users_quarantine_rules))
    )

@dlt.view
def valid_users_data():
    return spark.readStream.table("users_quarantine").filter("is_quarantined=false")

@dlt.table(
    name = "invalid_users_data",
)
def invalid_users_data():
    return spark.readStream.table("users_quarantine").filter("is_quarantined=true")

@dlt.view(
    name = "stg_users_transform"
)
def stg_users_transform():
    df = spark.readStream.table("valid_users_data")
    df = df.withColumnRenamed("id", "user_id")
    df = df.withColumn("birth_date", to_date(df["birth_date"],"yyyy-MM-dd"))
    df = df.drop("password")
    return df

dlt.create_streaming_table(
    name="transformed_users"
)

dlt.create_auto_cdc_flow(
    target = "transformed_users",
    source = "stg_users_transform",
    keys = ["user_id"],
    sequence_by = col("created_at"),
    apply_as_deletes = None,
    apply_as_truncates = None,
    except_column_list = ["is_quarantined"],
    stored_as_scd_type = 2
)





