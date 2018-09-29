from pyspark import SparkConf , SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf = conf)


def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

nameDict = sc.broadcast(loadMovieNames())

lines = sc.textFile("ml-100k/u.data")

movies = lines.map(lambda x:(int(x.split()[1]),1))
moviesCount = movies.reduceByKey(lambda x,y :x+y)

flipped = moviesCount.map(lambda xy: (xy[1],xy[0]))
sortedMovies = flipped.sortByKey()

sortedMoviesWithNames = sortedMovies.map(lambda countMovie : (nameDict.value[countMovie[1]], countMovie[0]))
results = sortedMoviesWithNames.collect()

for result in results:
    print(results)
