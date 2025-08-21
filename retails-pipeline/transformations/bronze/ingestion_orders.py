import dlt

@dlt.table(
    name="stg_orders"
)
def stg_orders():
    return spark.readStream.table("retails.source.orders")