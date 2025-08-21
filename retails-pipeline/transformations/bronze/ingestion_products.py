import dlt

@dlt.table(
    name="stg_products"
)
def stg_products():
    return spark.readStream.table("retails.source.products")