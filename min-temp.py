from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc  = SparkContext(conf = conf )

def parseLine(line):
    fields = line.split(',')
    station_id = fields[0]
    entryType = fields[2]
    temperature= float(fields[3]) * 0.1* (9.0/5.0)+32.0
    return (station_id, entryType, temperature)


lines = sc.textFile("1800.csv")
parsedLines = lines.map(parseLine)
minTemp = parsedLines.filter(lambda x:"TMIN" in x[1])
stationTemps = minTemp.map(lambda x:(x[0], x[2]) )
minTemp = stationTemps.reduceByKey(lambda x, y:min(x,y))

results = minTemp.collect()

for result in results:
    print(result[0]+ "\t{:.2f}F".format(result[1]))
