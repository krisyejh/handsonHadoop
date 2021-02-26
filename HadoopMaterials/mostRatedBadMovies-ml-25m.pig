ratings = LOAD '/user/raj_ops/ml-25m/ratings.csv' USING PigStorage(',') AS (userID:int, movieID:int, rating:int, ratingTime:int);
metadata = LOAD '/user/raj_ops/ml-25m/movies.csv' USING PigStorage(',') AS (movieID:int, movieTitle:chararray, genresType:chararray);
   
nameLookup = FOREACH metadata GENERATE movieID, movieTitle;
   
groupedRatings = GROUP ratings BY movieID;
avgRatings = FOREACH groupedRatings GENERATE group as movieID, AVG(ratings.rating) as avgRating, COUNT(ratings.rating) AS numRatings;

lowStarMovies = FILTER avgRatings BY avgRating < 2.0;
namedLowStarMovies = JOIN lowStarMovies BY movieID, nameLookup BY movieID;

finalResults = FOREACH namedLowStarMovies GENERATE nameLookup::movieTitle AS movieName, lowStarMovies::avgRating AS avgRating, lowStarMovies::numRatings AS numRatings;
finalResultsSorted = ORDER finalResults BY numRatings DESC;

DUMP finalResultsSorted;
