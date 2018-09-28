import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return  re.compile(r'\w+',re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("wordCounts")

sc = SparkContext(conf = conf)

input = sc.textFile("book.txt")
words = input.flatMap(normalizeWords)
wordCount = words.map(lambda x:(x,1)).reduceByKey(lambda x, y : x+y)
wordCountSorted= wordCount.map(lambda x:(x[1],x[0])).sortByKey()
results = wordCountSorted.collect()

for results in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
