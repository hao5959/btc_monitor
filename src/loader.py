from .spark_config import sparkSession


def loadCSVfrom(file_path: str):
    df = sparkSession.read.format("csv").option("header", True).option("inferSchema", True).load(file_path)
    return df

# df = loadCSVfrom(file_path="/Users/haogong/PycharmProjects/btc-spark/data/btc.csv")
# df.show()