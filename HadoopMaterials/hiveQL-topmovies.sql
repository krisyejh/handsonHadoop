CREATE VIEW IF NOT EXISTS topMovieIDs AS
SELECT movieID, count(MovieID) as ratingCount
FROM ratings
GROUP BY movieID
ORDER BY ratingCount DESC;

SELECT n.title, ratingCount
FROM topMovieIDs t JOIN names n ON t.movieID = n.movieID;
