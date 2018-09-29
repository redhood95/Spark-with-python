from pyspark import SparkConf , SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularSuperhero")
sc = SparkContext(conf = conf)

def countCoOccurrence(line):
    elements = line.split()
    return (int(element[0]), len(elements) -1)

def parseName(line):
    fields = line.split('\"')
    return (int(fields[0], fields[1].encode("utf8")))

names =  sc.textFile("data\marvel-names.txt")

namesRdd = names.map(parseName)
lines = sc.textFile("data\marvel-graph.txt")
pairing = lines.map(countCoOccurrence)

totalFriendsbyCharacter = pairing.reduceByKey(lambda x,y : x+y)
flipped = totalFriendsbyCharacter.map(lambda xy : (xy[1], xy[0]))

mostPopular = flipped.max()

mostPopularName = namesRdd.lookup(mostPopular[1])[0]
