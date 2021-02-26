

### HiveQL



hiveQL-topmovies.sql

```
CREATE VIEW topMovieIDs AS
SELECT movieID, count(MovieID) as ratingCount
FROM ratings
GROUP BY movieID
ORDER BY ratingCount DESC;

SELECT n.title, ratingCount
FROM topMovieIDs t JOIN names n ON t.movieID = n.movieID;
```





```
CREATE VIEW IF NOT EXISTS topMovieIDs AS
SELECT movieID, count(MovieID) as ratingCount
FROM ratings
GROUP BY movieID
ORDER BY ratingCount DESC;

SELECT n.title, ratingCount
FROM topMovieIDs t JOIN names n ON t.movieID = n.movieID;
```





Top Movies Sorted By Avarage Ratings

```
CREATE VIEW IF NOT EXISTS topMovieIDs AS
SELECT movieID, AVG(rating) as ratingAvg
FROM ratings
GROUP BY movieID
ORDER BY ratingAvg DESC;

SELECT n.title, ratingAvg
FROM topMovieIDs t JOIN names n ON t.movieID = n.movieID;
```



hiveQL-topmovies-sortByAvg.sql

```
CREATE VIEW IF NOT EXISTS topMoviesByAvg AS
SELECT movieID, AVG(rating) as ratingAvg, COUNT(MovieID) as ratingCount
FROM ratings
GROUP BY movieID
ORDER BY ratingAvg DESC;

SELECT n.title, ratingAvg 
FROM topMoviesByAvg t JOIN names n ON t.movieID = n.movieID
WHERE ratingCount > 10;
```





### Integrating with MySQL and Hadoop (Sqoop)



#### From MySQL to HDFS

```
CREATE database movielens;
SET NAMES 'utf8';
SET CHARACTER SET utf8;
use movielens;

source movielens.sql

```





```
SELECT movies.title, COUNT(ratings.movie_id) AS ratingCount 
FROM movies
INNER JOIN ratings
ON movies.id = ratings.movie_id
GROUP BY movies.title
ORDER BY ratingCount;
```



```
GRANT ALL PRIVILEGES ON movielens.* to 'root'@'localhost';
```

```
GRANT ALL PRIVILEGES ON movielens.* to ''@'localhost' identified by 'pass';
```

```
GRANT ALL PRIVILEGES ON movielens.* to ''@'localhost';
```





```
sqoop import --connect jdbc:mysql://localhost/movielens --driver com.mysql.jdbc.Driver --tables movies --username root --password <PASSWORD> -m 1 
```







#### From MySQL to Hive



```
CREATE USER 'raj_ops'@'localhost' IDENTIFIED BY 'raj_ops';
GRANT ALL PRIVILEGES ON movielens.* to 'raj_ops'@'localhost';
FLUSH PRIVILEGES;
```

```
sqoop import --connect jdbc:mysql://localhost/movielens --driver com.mysql.jdbc.Driver --table movies --username raj_ops --password raj_ops --hive-import -m 1
```







#### From HDFS to MySQL



```
use movielens;

CREATE TABLE exported_movies (id INTEGER, title VARCHAR(255), relearseDate DATE);


```



```
sqoop export --connect jdbc:mysql://localhost/movielens --driver com.mysql.jdbc.Driver -m 1 --table exported_movies --export-dir /apps/hive/warehouse/movies --input-fields-terminated-by '\0001'
```

```
sqoop export --connect jdbc:mysql://localhost/movielens --driver com.mysql.jdbc.Driver -m 1 --table exported_movies --export-dir /apps/hive/warehouse/movies --input-fields-terminated-by '\0001' --username root --password hadoop
```



