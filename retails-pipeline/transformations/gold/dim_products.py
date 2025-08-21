import dlt

@dlt.table(
    name = "dim_products"
)
def dim_products():
    df = spark.readStream.table("transformed_products")
    return df