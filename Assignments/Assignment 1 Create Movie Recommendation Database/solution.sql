CREATE TABLE users 
	(userid INTEGER,
	name TEXT,
	PRIMARY KEY (userid));

CREATE TABLE movies
	(movieid INTEGER,
	title TEXT,
	PRIMARY KEY (movieid));
	
CREATE TABLE taginfo
	(tagid INTEGER,
	content TEXT,
	PRIMARY KEY (tagid));
	
CREATE TABLE genres
	(genreid INTEGER,
	name TEXT,
	PRIMARY KEY (genreid));
	
CREATE TABLE ratings
	(userid INTEGER,
	movieid INTEGER,
	rating NUMERIC,
	timestamp BIGINT,
	PRIMARY KEY (userid, movieid),
	FOREIGN KEY (movieid) REFERENCES movies,
	CHECK (rating BETWEEN 0 AND 5));
	
CREATE TABLE tags
	(userid INTEGER NOT NULL,
	movieid INTEGER,
	tagid INTEGER,
	timestamp BIGINT,
	FOREIGN KEY (movieid) REFERENCES movies,
	FOREIGN KEY (userid) REFERENCES users,
	FOREIGN KEY (tagid) REFERENCES taginfo);
	
CREATE TABLE hasagenre
	(movieid INTEGER,
	genreid INTEGER,
	PRIMARY KEY (movieid, genreid),
	FOREIGN KEY (movieid) REFERENCES movies,
	FOREIGN KEY (genreid) REFERENCES genres);
