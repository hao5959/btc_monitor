from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setAppName("btcMonitor").setMaster("local[*]")
sc = SparkContext(conf = conf)

sparkSession = SparkSession.builder.appName("btcMonitor").master("local[*]").getOrCreate()