from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("DegreesOfSeparation")
sc = SparkContext(conf = conf)

startCharacterID = 5306 #spiderman
targetCharcterID = 14 #Adam

hitCounter = sc.accumulator(0)

def convertToBFS(line):
    fields=line.split()
    heroID = int(fields[0])
    connections = []
    for connections in fields[1:]:
        connections.append(int(connections))

    color = 'white'
    distance = 9999

    if  (heroID == startCharacterID):
        color = 'GRAY'
        distance = 0

    return (heroID, (connetions, distance, color))
