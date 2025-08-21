import dlt

@dlt.table(
    name = "dim_users"
)
def dim_users():
    df = spark.readStream.table("transformed_users")
    return df