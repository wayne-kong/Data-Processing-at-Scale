CREATE TABLE query1 AS 
SELECT name, COUNT(movieid) AS "moviecount" 
FROM genres, hasagenre 
WHERE genres.genreid=hasagenre.genreid 
GROUP BY name;

CREATE TABLE query2 AS
SELECT name, AVG(rating) as "rating" 
FROM genres, hasagenre,ratings
WHERE genres.genreid=hasagenre.genreid AND hasagenre.movieid=ratings.movieid
GROUP BY name;

CREATE TABLE query3 AS
SELECT title, COUNT(rating) AS "countofratings"
FROM movies, ratings
WHERE movies.movieid=ratings.movieid
GROUP BY title
HAVING COUNT(rating)>=10;

CREATE TABLE query4 AS 
SELECT movieid, title 
FROM movies 
WHERE movies.movieid in (SELECT movieid 
						FROM hasagenre,genres 
						WHERE hasagenre.genreid=genres.genreid AND genres.name='Comedy');


CREATE TABLE query5 AS 
SELECT title, AVG(rating) as "average" 
FROM movies, ratings 
WHERE movies.movieid=ratings.movieid 
GROUP BY movies.title;

CREATE TABLE query6 AS 
SELECT AVG(rating) AS "average" 
FROM ratings 
WHERE ratings.movieid in (SELECT movieid 
						FROM hasagenre, genres 
						WHERE hasagenre.genreid=genres.genreid ANd genres.name='Comedy');
						
						
CREATE TABLE query7 AS 
SELECT AVG(rating) AS "average" 
FROM ratings 
WHERE ratings.movieid in (SELECT movieid 
						FROM hasagenre, genres 
						WHERE hasagenre.genreid=genres.genreid AND genres.name='Comedy' 
						INTERSECT 
						SELECT movieid 
						FROM hasagenre, genres 
						WHERE hasagenre.genreid=genres.genreid AND genres.name='Romance');
						
CREATE TABLE query8 AS 
SELECT AVG(rating) AS "average" 
FROM ratings 
WHERE ratings.movieid in (SELECT movieid 
						FROM hasagenre, genres 
						WHERE hasagenre.genreid=genres.genreid AND genres.name='Romance') 
						AND ratings.movieid NOT in (SELECT movieid 
													FROM hasagenre, genres 
													WHERE hasagenre.genreid=genres.genreid AND genres.name='Comedy');
													
													
													
CREATE TABLE query9 AS 
SELECT movieid, rating 
FROM ratings 
WHERE userid=:v1 
GROUP BY userid, movieid;




CREATE TABLE avg_rating1 AS SELECT movies.movieid AS movieid1, AVG(rating) AS avgrating1 FROM movies, ratings WHERE movies.movieid=ratings.movieid GROUP BY movies.movieid;
CREATE TABLE avg_rating2 AS SELECT movies.movieid AS movieid2, AVG(rating) AS avgrating2 FROM movies, ratings WHERE movies.movieid=ratings.movieid GROUP BY movies.movieid;
CREATE TABLE avg_rating AS SELECT * FROM avg_rating1 CROSS JOIN avg_rating2;
CREATE TABLE similarity AS SELECT movieid1, movieid2, (1-ABS(avgrating1-avgrating2)/5) AS sim FROM avg_rating;
CREATE TABLE prerec1 AS SELECT movieid2 AS movieid, (sum(rating*sim)/sum(sim)) AS score FROM (SELECT movieid, rating FROM ratings WHERE userid=:v1) seed LEFT OUTER JOIN similarity b ON seed.movieid=b.movieid1 WHERE movieid1<>movieid2 GROUP by movieid2 ORDER by score DESC;
CREATE TABLE recommendation AS SELECT title FROM movies, prerec1 WHERE movies.movieid=prerec1.movieid AND score>3.9;

