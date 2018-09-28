from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc  = SparkContext(conf = conf )

def parseLine(line):
    fields = line.split(',')
    station_id = fields[0]
    entryType = fields[2]
    temperature= float(fields[3]) * 0.1 (9.0/5.0)+32.0
    return (station_id, entryType, temperature)


lines = sc.textFile("1800.csv")
