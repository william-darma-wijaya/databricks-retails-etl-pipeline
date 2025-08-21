import dlt

@dlt.table(
    name="stg_reviews",
)
def stg_reviews():
    return spark.readStream.table("retails.source.reviews")