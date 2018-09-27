from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf.setMaster("local").setAppName("ratingsHistogram")
sc = SparkContext(conf=conf)

num_lines = 
