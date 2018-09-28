import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return  re.compile(r'\w+',re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("wordCounts")
