from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf = conf)

lines = sc.textFile("ml-100k/u.data")
movies = lines.map(lambda x:(int(x.split()[1]),1))
moviesCount = movies.reduceByKey(lambda x,y :x+y)
flipped = moviesCount.map(lambda xy: (xy[1],xy[0]))
