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

def createStartingRdd():
    inputFile = sc.textFile("data/marvel-graph.txt")
    return inputFile.map(convertToBFS)

def bfsMap(node):
    characterID = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]

    results = []

    if (color == 'GRAY'):
        for connection in connections:
            newCharacterID = connection
            newDistance = distance + 1
            newColor = 'GRAY'
            if (targetCharacterID == connection):
                hitCounter.add(1)

            newEntry = (newCharacterID, ([], newDistance, newColor))
            results.append(newEntry)
        color = 'BLACK'

    #Emit the input node so we don't lose it.
    results.append( (characterID, (connections, distance, color)) )
    return results

def bfsReduce(data1, data2):
    edges1 = data1[0]
    edges2 = data2[0]
    distance1 = data1[1]
    distance2 = data2[1]
    color1 = data1[2]
    color2 = data2[2]

    distance = 9999
    color = color1
    edges = []

        if (len(edges1) > 0):
            edges.extend(edges1)
        if (len(edges2) > 0):
            edges.extend(edges2)

        if (distance1 < distance):
            distance = distance1

        if (distance2 < distance):
            distance = distance2


iterationRdd = createStartingRdd()

for iteration in range(0, 10):
    print("Running BFS iteration# " + str(iteration+1))

    mapped = iterationRdd.flatMap(bfsMap)
    print("Processing " + str(mapped.count()) + " values.")

    if (hitCounter.value > 0):
        print("Hit the target character! From " + str(hitCounter.value) \
            + " different direction(s).")
        break
    iterationRdd = mapped.reduceByKey(bfsReduce)
