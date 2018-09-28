from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("wordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("book.txt")

words = input.flatMap(lambda x:x.split())
wordCounts = words.countByValue()
