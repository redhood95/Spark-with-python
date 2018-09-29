from pyspark import SparkConf , SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularSuperhero")
sc = SparkContext(conf = conf)

def countCoOccurrence(line):
    elements = line.split()
    return (int(element[0]), len(elements) -1)

def parseName(line):
    fields = line.split('\"')
    return (int(fields[0], fields[1].encode("utf8")))

names =  sc.textFile("marvel-names.txt")
