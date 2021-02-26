

#### The Worst Movies





LowestRatedMovieSpark.py

```
from pyspark import SparkConf, SparkContext

# This function just creates a Python "dictionary" we can later
# use to convert movie ID's to movie names while printing out
# the final results.
def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

# Take each line of u.data and convert it to (movieID, (rating, 1.0))
# This way we can then add up all the ratings for each movie, and
# the total number of ratings for each movie (which lets us compute the average)
def parseInput(line):
    fields = line.split()
    return (int(fields[1]), (float(fields[2]), 1.0))

if __name__ == "__main__":
    # The main script - create our SparkContext
    conf = SparkConf().setAppName("WorstMovies")
    sc = SparkContext(conf = conf)

    # Load up our movie ID -> movie name lookup table
    movieNames = loadMovieNames()

    # Load up the raw u.data file
    lines = sc.textFile("hdfs:///user/maria_dev/ml-100k/u.data")

    # Convert to (movieID, (rating, 1.0))
    movieRatings = lines.map(parseInput)

    # Reduce to (movieID, (sumOfRatings, totalRatings))
    ratingTotalsAndCount = movieRatings.reduceByKey(lambda movie1, movie2: ( movie1[0] + movie2[0], movie1[1] + movie2[1] ) )

    # Map to (movieID, averageRating)
    averageRatings = ratingTotalsAndCount.mapValues(lambda totalAndCount : totalAndCount[0] / totalAndCount[1])

    # Sort by average rating
    sortedMovies = averageRatings.sortBy(lambda x: x[1])

    # Take the top 10 results
    results = sortedMovies.take(10)

    # Print them out:
    for result in results:
        print(movieNames[result[0]], result[1])

```



```
spark-submit LowestRatedMovieSpark.py
```

Results are:

```
('3 Ninjas: High Noon At Mega Mountain (1998)', 1.0)
('Beyond Bedlam (1993)', 1.0)
('Power 98 (1995)', 1.0)
('Bloody Child, The (1996)', 1.0)
('Amityville: Dollhouse (1996)', 1.0)
('Babyfever (1994)', 1.0)
('Homage (1995)', 1.0)
('Somebody to Love (1994)', 1.0)
('Crude Oasis, The (1995)', 1.0)
('Every Other Weekend (1990)', 1.0)
```







LowestRatedMovieSpark-ml-25m.py

```
from pyspark import SparkConf, SparkContext
from itertools import islice

# This function just creates a Python "dictionary" we can later
# use to convert movie ID's to movie names while printing out
# the final results.
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

# Take each line of u.data and convert it to (movieID, (rating, 1.0))
# This way we can then add up all the ratings for each movie, and
# the total number of ratings for each movie (which lets us compute the average)
def parseInput(line):
    fields = line.split(',')
    return (int(fields[1]), (float(fields[2]), 1.0))

if __name__ == "__main__":
    # The main script - create our SparkContext
    conf = SparkConf().setAppName("WorstMovies")
    sc = SparkContext(conf = conf)

    # Load up our movie ID -> movie name lookup table
    movieNames = loadMovieNames()

    # Load up the raw u.data file
    lines = sc.textFile("hdfs:///user/raj_ops/ml-25m/ratings.csv")

    # Skip header line in CSV
    lines = lines.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it )


    # Convert to (movieID, (rating, 1.0))
    movieRatings = lines.map(parseInput)

    # Reduce to (movieID, (sumOfRatings, totalRatings))
    ratingTotalsAndCount = movieRatings.reduceByKey(lambda movie1, movie2: ( movie1[0] + movie2[0], movie1[1] + movie2[1] ) )

    # Map to (movieID, averageRating)
    averageRatings = ratingTotalsAndCount.mapValues(lambda totalAndCount : totalAndCount[0] / totalAndCount[1])

    # Sort by average rating
    sortedMovies = averageRatings.sortBy(lambda x: x[1])

    # Take the top 10 results
    results = sortedMovies.take(10)

    # Print them out:
    for result in results:
        print(movieNames[result[0]], result[1])
```



Results are:

```
('Old Man', 0.5)
('Santa Jaws (2018)', 0.5)
('Truth or Double Dare (2018)', 0.5)
('Born Reckless (1958)', 0.5)
('The 3 Little Pigs: The Movie (1996)', 0.5)
('Zarkorr! The Invader (1996)', 0.5)
('Bellini e o Dem\xc3\xb4nio (2008)', 0.5)
('Betty Blowtorch: And Her Amazing True Life Adventures (2003)', 0.5)
('Swamp Ape (2017)', 0.5)
('Loving Cuba (2019)', 0.5)
```





### DataFrame



Ml-100k  LowestRatedMovieDataFrame.py

```
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

def parseInput(line):
    fields = line.split()
    return Row(movieID = int(fields[1]), rating = float(fields[2]))

if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

    # Load up our movie ID -> name dictionary
    movieNames = loadMovieNames()

    # Get the raw data
    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/u.data")
   
    
    # Convert it to a RDD of Row objects with (movieID, rating)
    movies = lines.map(parseInput)
    # Convert that to a DataFrame
    movieDataset = spark.createDataFrame(movies)

    # Compute average rating for each movieID
    averageRatings = movieDataset.groupBy("movieID").avg("rating")

    # Compute count of ratings for each movieID
    counts = movieDataset.groupBy("movieID").count()

    # Join the two together (We now have movieID, avg(rating), and count columns)
    averagesAndCounts = counts.join(averageRatings, "movieID")

    # Pull the top 10 results
    topTen = averagesAndCounts.orderBy("avg(rating)").take(10)

    # Print them out, converting movie ID's to names as we go.
    for movie in topTen:
        print (movieNames[movie[0]], movie[1], movie[2])

    # Stop the session
    spark.stop()
```



Results are:

```
('Amityville: A New Generation (1993)', 5, 1.0)
('Hostile Intentions (1994)', 1, 1.0)
('Lotto Land (1995)', 1, 1.0)
('Careful (1992)', 1, 1.0)
('Falling in Love Again (1980)', 2, 1.0)
('Amityville: Dollhouse (1996)', 3, 1.0)
('Power 98 (1995)', 1, 1.0)
('Low Life, The (1994)', 1, 1.0)
('Further Gesture, A (1996)', 1, 1.0)
('Touki Bouki (Journey of the Hyena) (1973)', 1, 1.0)
```





ml-25m vi LowestRatedMovieDataFrame-ml-25m.py

```
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions
from itertools import islice

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

    # Get the raw data
    lines = spark.sparkContext.textFile("hdfs:///user/raj_ops/ml-25m/ratings.csv")
    
    # Skip header line in CSV
    lines = lines.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it )
    
    # Convert it to a RDD of Row objects with (movieID, rating)
    movies = lines.map(parseInput)
    # Convert that to a DataFrame
    movieDataset = spark.createDataFrame(movies)

    # Compute average rating for each movieID
    averageRatings = movieDataset.groupBy("movieID").avg("rating")

    # Compute count of ratings for each movieID
    counts = movieDataset.groupBy("movieID").count()

    # Join the two together (We now have movieID, avg(rating), and count columns)
    averagesAndCounts = counts.join(averageRatings, "movieID")

    # Pull the top 10 results
    topTen = averagesAndCounts.orderBy("avg(rating)").take(10)

    # Print them out, converting movie ID's to names as we go.
    for movie in topTen:
        print (movieNames[movie[0]], movie[1], movie[2])

    # Stop the session
    spark.stop()
```



Results are:

```
('Le rose del deserto (2006)', 1, 0.5)
('Cult (2013)', 1, 0.5)
("L'ann\xc3\xa9e prochaine... si tout va bien (1981)", 1, 0.5)
('The Train: Some Lines Shoulder Never Be Crossed... (2007)', 2, 0.5)
('Kalashnikov (2014)', 2, 0.5)
('Naisen logiikka (1999)', 1, 0.5)
('A Brief Season (1969)', 1, 0.5)
('Around June (2008)', 1, 0.5)
('Music and Apocalypse (2019)', 1, 0.5)
('The 12 Disasters of Christmas (2012)', 1, 0.5)
```





ml-25m LowestRatedMovieDataFrame-ml-25m-csvrread.py

```
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
    
    # convert movieId, ratings to Integer
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
```





Results are: ???

```
('1013 Briar Lane', 1, 0.0)
('White Bondage (1937)', 1, 0.0)
('Black Sabbath: The End of The End (2017)', 1, 0.0)
('The Documentary of Ozbo (2014)', 1, 0.0)
('Paranormal Farm (2017)', 1, 0.0)
('A Grim Becoming', 1, 0.0)
('Como Se Tornar o Pior Aluno da Escola (2017)', 1, 0.0)
('Hole In The Wall (2014)', 1, 0.0)
('A Haunting On Dice Road 2: Town of the Dead (2017)', 1, 0.0)
('The Last Whistle (2018)', 4, 0.0)
```





### ML ALS



##### Notice: Enable Spark crossjoin

`spark.sql.crossJoin.enabled=true`



Ml-100k MovieRecommendationsALS.py

```
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql.functions import lit

# Load up movie ID -> movie name dictionary
def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1].decode('ascii', 'ignore')
    return movieNames

# Convert u.data lines into (userID, movieID, rating) rows
def parseInput(line):
    fields = line.value.split()
    return Row(userID = int(fields[0]), movieID = int(fields[1]), rating = float(fields[2]))


if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder.appName("MovieRecs").getOrCreate()

    # Load up our movie ID -> name dictionary
    movieNames = loadMovieNames()

    # Get the raw data
    lines = spark.read.text("hdfs:///user/maria_dev/ml-100k/u.data").rdd

    # Convert it to a RDD of Row objects with (userID, movieID, rating)
    ratingsRDD = lines.map(parseInput)

    # Convert to a DataFrame and cache it
    ratings = spark.createDataFrame(ratingsRDD).cache()

    # Create an ALS collaborative filtering model from the complete data set
    als = ALS(maxIter=5, regParam=0.01, userCol="userID", itemCol="movieID", ratingCol="rating")
    model = als.fit(ratings)

    # Print out ratings from user 0:
    print("\nRatings for user ID 0:")
    userRatings = ratings.filter("userID = 0")
    for rating in userRatings.collect():
        print movieNames[rating['movieID']], rating['rating']

    print("\nTop 20 recommendations:")
    # Find movies rated more than 100 times
    ratingCounts = ratings.groupBy("movieID").count().filter("count > 100")
    # Construct a "test" dataframe for user 0 with every movie rated more than 100 times
    popularMovies = ratingCounts.select("movieID").withColumn('userID', lit(0))

    # Run our model on that list of popular movies for user ID 0
    recommendations = model.transform(popularMovies)

    # Get the top 20 movies with the highest predicted rating for this user
    topRecommendations = recommendations.sort(recommendations.prediction.desc()).take(20)

    for recommendation in topRecommendations:
        print (movieNames[recommendation['movieID']], recommendation['prediction'])

    spark.stop()
```



Rusults are:

```
Ratings for user ID 0:
Star Wars (1977) 5.0
Empire Strikes Back, The (1980) 5.0
Gone with the Wind (1939) 1.0

Top 20 recommendations:
(u'Wrong Trousers, The (1993)', 5.749821662902832)
(u'Fifth Element, The (1997)', 5.232528209686279)
(u'Close Shave, A (1995)', 5.050625324249268)
(u'Monty Python and the Holy Grail (1974)', 4.99659538269043)
(u'Star Wars (1977)', 4.98954963684082)
(u'Army of Darkness (1993)', 4.980320930480957)
(u'Empire Strikes Back, The (1980)', 4.972929954528809)
(u'Princess Bride, The (1987)', 4.957705497741699)
(u'Blade Runner (1982)', 4.910674571990967)
(u'Return of the Jedi (1983)', 4.77808141708374)
(u'Rumble in the Bronx (1995)', 4.69175910949707)
(u'Raiders of the Lost Ark (1981)', 4.636718273162842)
(u"Jackie Chan's First Strike (1996)", 4.632108211517334)
(u'Twelve Monkeys (1995)', 4.614840507507324)
(u'Spawn (1997)', 4.57417106628418)
(u'Terminator, The (1984)', 4.561151027679443)
(u'Alien (1979)', 4.54151725769043)
(u'Terminator 2: Judgment Day (1991)', 4.529487133026123)
(u'Usual Suspects, The (1995)', 4.517911911010742)
(u'Mystery Science Theater 3000: The Movie (1996)', 4.5095906257629395)
```









Ml-25m MovieRecommendationsALS-ml-25m.py

25m 数据集要求内存较多，可以采用 ml-latest-small 数据集

```
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql.functions import lit
from itertools import islice

# Load up movie ID -> movie name dictionary
def loadMovieNames():
    movieNames = {}
    with open("ml-25m/movies.csv") as f:
        for line in f:
            fields = line.split(',')
            try:
              int(fields[0])
            except ValueError:
              continue
            movieNames[int(fields[0])] = fields[1].decode('ascii', 'ignore')
    return movieNames

# Convert u.data lines into (userID, movieID, rating) rows
def parseInput(line):
    fields = line.value.split(',')
    return Row(userID = int(fields[0]), movieID = int(fields[1]), rating = float(fields[2]))


if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder.appName("MovieRecs").getOrCreate()

    # Load up our movie ID -> name dictionary
    movieNames = loadMovieNames()

    # Get the raw data
    lines = spark.read.text("hdfs:///user/raj_ops/ml-25m/ratings.csv").rdd

    # Skip header line in CSV
    lines = lines.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it )
    
    # Convert it to a RDD of Row objects with (userID, movieID, rating)
    ratingsRDD = lines.map(parseInput)

    # Convert to a DataFrame and cache it
    ratings = spark.createDataFrame(ratingsRDD).cache()

    # Create an ALS collaborative filtering model from the complete data set
    als = ALS(maxIter=5, regParam=0.01, userCol="userID", itemCol="movieID", ratingCol="rating")
    model = als.fit(ratings)

    # Print out ratings from user 1:
    print("\nRatings for user ID 1:")
    userRatings = ratings.filter("userID = 1")
    for rating in userRatings.collect():
        print movieNames[rating['movieID']], rating['rating']

    print("\nTop 20 recommendations:")
    # Find movies rated more than 100 times
    ratingCounts = ratings.groupBy("movieID").count().filter("count > 100")
    # Construct a "test" dataframe for user 1 with every movie rated more than 100 times
    popularMovies = ratingCounts.select("movieID").withColumn('userID', lit(1))

    # Run our model on that list of popular movies for user ID 1
    recommendations = model.transform(popularMovies)

    # Get the top 20 movies with the highest predicted rating for this user
    topRecommendations = recommendations.sort(recommendations.prediction.desc()).take(20)

    for recommendation in topRecommendations:
        print (movieNames[recommendation['movieID']], recommendation['prediction'])

    spark.stop()
```



Results are:

```
Top 20 recommendations:
(u'"Shawshank Redemption', 5.348514080047607)
(u'Lon: The Professional (a.k.a. The Professional) (Lon) (1994)', 5.306513786315918)
(u'Willy Wonka & the Chocolate Factory (1971)', 5.255984306335449)
(u'American History X (1998)', 5.2534871101379395)
(u'"Amelie (Fabuleux destin d\'Amlie Poulain', 5.23676872253418)
(u'Fight Club (1999)', 5.218761444091797)
(u'Good Will Hunting (1997)', 5.196744918823242)
(u'"Departed', 5.184854507446289)
(u'Up (2009)', 5.1776299476623535)
(u'Pulp Fiction (1994)', 5.16886568069458)
(u'"Godfather', 5.1680145263671875)
(u'"Green Mile', 5.164059638977051)
(u'"Silence of the Lambs', 5.152304172515869)
(u'"Princess Bride', 5.137851715087891)
(u'Eternal Sunshine of the Spotless Mind (2004)', 5.122262954711914)
(u'Finding Nemo (2003)', 5.113361835479736)
(u'Forrest Gump (1994)', 5.105884075164795)
(u'Memento (2000)', 5.101924896240234)
(u'"Usual Suspects', 5.093531131744385)
(u"Schindler's List (1993)", 5.067870140075684)
```









Ml-25m MovieRecommendationsALS-ml-25m-csvread.py

```
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql.functions import lit
from pyspark.sql.types import IntegerType

# Load up movie ID -> movie name dictionary
def loadMovieNames():
    movieNames = {}
    with open("ml-25m/movies.csv") as f:
        for line in f:
            fields = line.split(',')
            try:
              int(fields[0])
            except ValueError:
              continue
            movieNames[int(fields[0])] = fields[1].decode('ascii', 'ignore')
    return movieNames

# Convert u.data lines into (userID, movieID, rating) rows
def parseInput(line):
    fields = line.value.split(',')
    return Row(userID = int(fields[0]), movieID = int(fields[1]), rating = float(fields[2]))


if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder.appName("MovieRecs").getOrCreate()

    # Load up our movie ID -> name dictionary
    movieNames = loadMovieNames()

    # Read CSV data with spark 2.0 csv read   
    ratings = spark.read.csv("hdfs:///user/raj_ops/ml-latest-small/ratings.csv", header=True)
    
    # convert movieId, ratings to Integer
    ratings = ratings.withColumn("rating", ratings["rating"].cast(IntegerType()))
    ratings = ratings.withColumn("movieId", ratings["movieId"].cast(IntegerType()))
    ratings = ratings.withColumn("userId", ratings["movieId"].cast(IntegerType()))
    
    # Get the raw data
    #lines = spark.read.text("hdfs:///user/raj_ops/ml-25m/ratings.csv").rdd

    # Skip header line in CSV
    # lines = lines.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it )
    
    # Convert it to a RDD of Row objects with (userID, movieID, rating)
    # ratingsRDD = lines.map(parseInput)

    # Convert to a DataFrame and cache it
    # ratings = spark.createDataFrame(ratingsRDD).cache()

    # Create an ALS collaborative filtering model from the complete data set
    als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating")
    model = als.fit(ratings)

    # Print out ratings from user 1:
    print("\nRatings for user ID 1:")
    userRatings = ratings.filter("userId = 1")
    for rating in userRatings.collect():
        print movieNames[rating['movieId']], rating['rating']

    print("\nTop 20 recommendations:")
    # Find movies rated more than 100 times
    ratingCounts = ratings.groupBy("movieId").count().filter("count > 100")
    # Construct a "test" dataframe for user 1 with every movie rated more than 100 times
    popularMovies = ratingCounts.select("movieId").withColumn('userId', lit(1))

    # Run our model on that list of popular movies for user ID 1
    recommendations = model.transform(popularMovies)

    # Get the top 20 movies with the highest predicted rating for this user
    topRecommendations = recommendations.sort(recommendations.prediction.desc()).take(20)

    for recommendation in topRecommendations:
        print (movieNames[recommendation['movieId']], recommendation['prediction'])

    spark.stop()
```



结果有点奇怪：

```
Ratings for user ID 1:
Toy Story (1995) 4
Toy Story (1995) 4
Toy Story (1995) 4
Toy Story (1995) 2
Toy Story (1995) 4
Toy Story (1995) 3
Toy Story (1995) 4
Toy Story (1995) 3
Toy Story (1995) 3
Toy Story (1995) 5
...
...

Top 20 recommendations:
(u'Toy Story (1995)', 3.8110861778259277)
(u'Up (2009)', 2.733916759490967)
(u'"Usual Suspects', 2.5129928588867188)
(u'Mission: Impossible (1996)', 2.4754080772399902)
(u'American Beauty (1999)', 2.3767316341400146)
(u'Reservoir Dogs (1992)', 2.3078253269195557)
(u'Lon: The Professional (a.k.a. The Professional) (Lon) (1994)', 2.240504026412964)
(u'"Bourne Identity', 2.233828544616699)
(u'Die Hard: With a Vengeance (1995)', 2.2239818572998047)
(u'Aladdin (1992)', 2.2225074768066406)
(u'Four Weddings and a Funeral (1994)', 2.1887381076812744)
(u'"Shining', 2.0235111713409424)
(u'Indiana Jones and the Last Crusade (1989)', 2.0148839950561523)
(u'Harry Potter and the Chamber of Secrets (2002)', 1.9384325742721558)
(u'Twister (1996)', 1.9015357494354248)
(u'"Amelie (Fabuleux destin d\'Amlie Poulain', 1.8677514791488647)
(u'Batman Begins (2005)', 1.7644309997558594)
(u'Dumb & Dumber (Dumb and Dumber) (1994)', 1.6858024597167969)
(u'"Sixth Sense', 1.6345598697662354)
(u'Indiana Jones and the Temple of Doom (1984)', 1.6344879865646362)
```









Ml-100k LowestRatedPopularMovieSpark.py

```
from pyspark import SparkConf, SparkContext

# This function just creates a Python "dictionary" we can later
# use to convert movie ID's to movie names while printing out
# the final results.
def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

# Take each line of u.data and convert it to (movieID, (rating, 1.0))
# This way we can then add up all the ratings for each movie, and
# the total number of ratings for each movie (which lets us compute the average)
def parseInput(line):
    fields = line.split()
    return (int(fields[1]), (float(fields[2]), 1.0))

if __name__ == "__main__":
    # The main script - create our SparkContext
    conf = SparkConf().setAppName("WorstMovies")
    sc = SparkContext(conf = conf)

    # Load up our movie ID -> movie name lookup table
    movieNames = loadMovieNames()

    # Load up the raw u.data file
    lines = sc.textFile("hdfs:///user/maria_dev/ml-100k/u.data")

    # Convert to (movieID, (rating, 1.0))
    movieRatings = lines.map(parseInput)

    # Reduce to (movieID, (sumOfRatings, totalRatings))
    ratingTotalsAndCount = movieRatings.reduceByKey(lambda movie1, movie2: ( movie1[0] + movie2[0], movie1[1] + movie2[1] ) )

    # Filter out movies rated 10 or fewer times
    popularTotalsAndCount = ratingTotalsAndCount.filter(lambda x: x[1][1] > 10)

    # Map to (movieID, averageRating)
    averageRatings = popularTotalsAndCount.mapValues(lambda totalAndCount : totalAndCount[0] / totalAndCount[1])

    # Sort by average rating
    sortedMovies = averageRatings.sortBy(lambda x: x[1])

    # Take the top 10 results
    results = sortedMovies.take(10)

    # Print them out:
    for result in results:
        print(movieNames[result[0]], result[1])
```



Results are:

```
SPARK_MAJOR_VERSION is set to 2, using Spark2
('Children of the Corn: The Gathering (1996)', 1.3157894736842106)
('Body Parts (1991)', 1.6153846153846154)
('Amityville II: The Possession (1982)', 1.6428571428571428)
('Lawnmower Man 2: Beyond Cyberspace (1996)', 1.7142857142857142)
('Robocop 3 (1993)', 1.7272727272727273)
('Free Willy 3: The Rescue (1997)', 1.7407407407407407)
("Gone Fishin' (1997)", 1.8181818181818181)
('Solo (1996)', 1.8333333333333333)
('Ready to Wear (Pret-A-Porter) (1994)', 1.8333333333333333)
('Vampire in Brooklyn (1995)', 1.8333333333333333)
```







Ml-25m LowestRatedPopularMovieSpark-ml-25m.py



```
from pyspark import SparkConf, SparkContext
from itertools import islice

# This function just creates a Python "dictionary" we can later
# use to convert movie ID's to movie names while printing out
# the final results.
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

# Take each line of u.data and convert it to (movieID, (rating, 1.0))
# This way we can then add up all the ratings for each movie, and
# the total number of ratings for each movie (which lets us compute the average)
def parseInput(line):
    fields = line.split(',')
    return (int(fields[1]), (float(fields[2]), 1.0))

if __name__ == "__main__":
    # The main script - create our SparkContext
    conf = SparkConf().setAppName("WorstMovies")
    sc = SparkContext(conf = conf)

    # Load up our movie ID -> movie name lookup table
    movieNames = loadMovieNames()

    # Load up the raw u.data file
    lines = sc.textFile("hdfs:///user/raj_ops/ml-25m/ratings.csv")


    lines = lines.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it )
#    header = lines.first()
#    lines = lines.filter(row => row != header)

    # Convert to (movieID, (rating, 1.0))
    movieRatings = lines.map(parseInput)

    # Reduce to (movieID, (sumOfRatings, totalRatings))
    ratingTotalsAndCount = movieRatings.reduceByKey(lambda movie1, movie2: ( movie1[0] + movie2[0], movie1[1] + movie2[1] ) )

# Filter out movies rated 10 or fewer times
    popularTotalsAndCount = ratingTotalsAndCount.filter(lambda x: x[1][1] > 10)


    # Map to (movieID, averageRating)
    averageRatings = ratingTotalsAndCount.mapValues(lambda totalAndCount : totalAndCount[0] / totalAndCount[1])

    # Sort by average rating
    sortedMovies = averageRatings.sortBy(lambda x: x[1])

    # Take the top 10 results
    results = sortedMovies.take(10)

    # Print them out:
    for result in results:
        print(movieNames[result[0]], result[1])
```



Results are:

```
('Old Man', 0.5)
('Santa Jaws (2018)', 0.5)
('Truth or Double Dare (2018)', 0.5)
('Born Reckless (1958)', 0.5)
('The 3 Little Pigs: The Movie (1996)', 0.5)
('Zarkorr! The Invader (1996)', 0.5)
('Bellini e o Dem\xc3\xb4nio (2008)', 0.5)
('Betty Blowtorch: And Her Amazing True Life Adventures (2003)', 0.5)
('Swamp Ape (2017)', 0.5)
('Loving Cuba (2019)', 0.5)
```







Ml-100k LowestRatedPopularMovieDataFrame.py



```
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

def parseInput(line):
    fields = line.split()
    return Row(movieID = int(fields[1]), rating = float(fields[2]))

if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

    # Load up our movie ID -> name dictionary
    movieNames = loadMovieNames()

    # Get the raw data
    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/u.data")
    # Convert it to a RDD of Row objects with (movieID, rating)
    movies = lines.map(parseInput)
    # Convert that to a DataFrame
    movieDataset = spark.createDataFrame(movies)

    # Compute average rating for each movieID
    averageRatings = movieDataset.groupBy("movieID").avg("rating")

    # Compute count of ratings for each movieID
    counts = movieDataset.groupBy("movieID").count()

    # Join the two together (We now have movieID, avg(rating), and count columns)
    averagesAndCounts = counts.join(averageRatings, "movieID")

    # Filter movies rated 10 or fewer times
    popularAveragesAndCounts = averagesAndCounts.filter("count > 10")

    # Pull the top 10 results
    topTen = popularAveragesAndCounts.orderBy("avg(rating)").take(10)

    # Print them out, converting movie ID's to names as we go.
    for movie in topTen:
        print (movieNames[movie[0]], movie[1], movie[2])

    # Stop the session
    spark.stop()
```





Ml-25m LowestRatedPopularMovieDataFrame-ml-25m.py



```
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions
from itertools import islice

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

    # Get the raw data
    lines = spark.sparkContext.textFile("hdfs:///user/raj_ops/ml-25m/ratings.csv")

    # Skip header line in CSV
    lines = lines.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it )

    # Convert it to a RDD of Row objects with (movieID, rating)
    movies = lines.map(parseInput)
    # Convert that to a DataFrame
    movieDataset = spark.createDataFrame(movies)

    # Compute average rating for each movieID
    averageRatings = movieDataset.groupBy("movieID").avg("rating")

    # Compute count of ratings for each movieID
    counts = movieDataset.groupBy("movieID").count()

    # Join the two together (We now have movieID, avg(rating), and count columns)
    averagesAndCounts = counts.join(averageRatings, "movieID")

    # Filter movies rated 10 or fewer times
    popularAveragesAndCounts = averagesAndCounts.filter("count > 10")

    # Pull the top 10 results
    topTen = averagesAndCounts.orderBy("avg(rating)").take(10)

    # Print them out, converting movie ID's to names as we go.
    for movie in topTen:
        print (movieNames[movie[0]], movie[1], movie[2])

    # Stop the session
    spark.stop()
```



Results are:

```
('Cult (2013)', 1, 0.5)
('A Brief Season (1969)', 1, 0.5)
("L'ann\xc3\xa9e prochaine... si tout va bien (1981)", 1, 0.5)
('Kalashnikov (2014)', 2, 0.5)
('The Train: Some Lines Shoulder Never Be Crossed... (2007)', 2, 0.5)
('Burning Down the House (2001)', 1, 0.5)
('The 12 Disasters of Christmas (2012)', 1, 0.5)
('Le rose del deserto (2006)', 1, 0.5)
('"Paimen', 1, 0.5)
('Naisen logiikka (1999)', 1, 0.5)
```

