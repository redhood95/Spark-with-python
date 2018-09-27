from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf.setMaster("local").setAppName("ratingsHistogram")
sc = SparkContext(conf=conf)

num_lines = sc.text("ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))

for key, values in sortedResults.iteritems():
	print("%s %i" % (key, value))
