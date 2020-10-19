from src.loader import loadCSVfrom
from pyspark.sql import DataFrame
from pyspark.sql import functions as func


df = loadCSVfrom(file_path="data/btc.csv")
# def aggregrateBlocks(df: DataFrame):
#     df.agg(func.sum("blockCount"))
# print("total record count: %d" % (df.count()))
# aggregrateBlocks(df)
df = df.withColumn("date", df["date"].cast("Date"))
df = df \
        .withColumn("year", func.year(df["date"])) \
        .withColumn("month", func.month(df["date"]))
print('schema', df.schema)

def showBlocksize(df: DataFrame):
    df \
        .groupBy(df["year"], df["month"]) \
        .agg(func.sum(df["blocksize"])) \
        .sort(df["year"], df["month"]) \
        .filter(df["year"].isin("2018", "2019")) \
        .show()

showBlocksize(df)

# df \
#         .groupBy(func.year("date"), func.month("date")) \
#         .agg(func.sum("blocksize")) \
#         .sort(func.year("date"), func.month("date")) \
#         .filter(func.year(df["date"]) == "2009") \
#         .show()

