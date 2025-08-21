import dlt

@dlt.table(
    name = "fact_reviews"
)
def fact_reviews():
    df = spark.readStream.table("transformed_reviews")
    return df