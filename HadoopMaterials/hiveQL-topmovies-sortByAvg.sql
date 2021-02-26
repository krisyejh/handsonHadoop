CREATE VIEW IF NOT EXISTS topMoviesByAvg AS
SELECT movieID, AVG(rating) as ratingAvg, COUNT(MovieID) as ratingCount
FROM ratings
GROUP BY movieID
ORDER BY ratingAvg DESC;

SELECT n.title, ratingAvg 
FROM topMoviesByAvg t JOIN names n ON t.movieID = n.movieID
WHERE ratingCount > 10;
