from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions
from pyspark.sql.types import IntegerType

def loadMovieNames():
    movieNames = {}
    with open("ml-25m/movies.csv") as f:
        for line in f:
            fields = line.split(',')
            try:
              int(fields[0])
            except ValueError:
              continue
            movieNames[int(fields[0])] = fields[1]
    return movieNames

def parseInput(line):
    fields = line.split(',')
    return Row(movieID = int(fields[1]), rating = float(fields[2]))


if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

    # Load up our movie ID -> name dictionary
    movieNames = loadMovieNames()

    # Read CSV data with spark 2.0 csv read   
    movieDataFrame = spark.read.csv("hdfs:///user/raj_ops/ml-25m/ratings.csv", header=True)
    
    # convert ratings to Integer
    movieDataFrame = movieDataFrame.withColumn("rating", movieDataFrame["rating"].cast(IntegerType()))
    movieDataFrame = movieDataFrame.withColumn("movieId", movieDataFrame["movieId"].cast(IntegerType()))

    # Compute average rating for each movieID
    averageRatings = movieDataFrame.groupBy("movieId").avg("rating")

    # Compute count of ratings for each movieID
    counts = movieDataFrame.groupBy("movieId").count()

    # Join the two together (We now have movieID, avg(rating), and count columns)
    averagesAndCounts = counts.join(averageRatings, "movieId")

    # Pull the top 10 results
    topTen = averagesAndCounts.orderBy("avg(rating)").take(10)

    # Print them out, converting movie ID's to names as we go.
    for movie in topTen:
        print (movieNames[movie[0]], movie[1], movie[2])

    # Stop the session
    spark.stop()
