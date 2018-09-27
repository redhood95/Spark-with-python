from pyspark import SparkConf, SparkContext
import collections

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))


num_lines = sc.textFile("ml-100k/u.data")
ratings = num_lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))

for key, values in sortedResults.items():
	print("%s %i" % (key, values))
