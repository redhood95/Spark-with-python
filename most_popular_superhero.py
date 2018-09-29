from pyspark import SparkConf , SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularSuperhero")
sc = SparkContext(conf = conf)

names =  sc.textFile("marvel-names.txt")
