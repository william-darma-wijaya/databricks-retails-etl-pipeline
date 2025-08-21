import dlt

@dlt.table(
    name="stg_users"
)
def stg_users():
    return spark.readStream.table("retails.source.users")