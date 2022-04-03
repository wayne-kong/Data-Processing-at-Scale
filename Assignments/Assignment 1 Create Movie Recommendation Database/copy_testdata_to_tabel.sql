COPY users FROM 'D:\Computer Science\Data Processing at Scale CSE 511\TestData\users.dat.txt' DELIMITER '%';
SET client_encoding TO 'UTF8';
COPY movies FROM 'D:\Computer Science\Data Processing at Scale CSE 511\TestData\movies.dat.txt' DELIMITER '%';
COPY genres FROM 'D:\Computer Science\Data Processing at Scale CSE 511\TestData\genres.dat.txt' DELIMITER '%';
COPY ratings FROM 'D:\Computer Science\Data Processing at Scale CSE 511\TestData\ratings.dat.txt' DELIMITER '%';
COPY hasagenre FROM 'D:\Computer Science\Data Processing at Scale CSE 511\TestData\hasagenre.dat.txt' DELIMITER '%';
COPY taginfo FROM 'D:\Computer Science\Data Processing at Scale CSE 511\TestData\taginfo.dat.txt' DELIMITER '%';
COPY tags FROM 'D:\Computer Science\Data Processing at Scale CSE 511\TestData\tags.dat.txt' DELIMITER '%';