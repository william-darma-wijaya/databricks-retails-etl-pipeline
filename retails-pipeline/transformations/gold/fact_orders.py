import dlt

@dlt.table(
    name = "fact_orders"
)
def fact_orders():
    df = spark.readStream.table("transformed_orders")
    return df