import psycopg2

con=psycopg2.connect(host="localhost", database="datafragmentation", user="postgres", password="Boeing2018")
cur=con.cursor()

cur.execute("CREATE TABLE ratings (userid Int, movieid int, rating REAL, timestamp BIGINT, PRIMARY KEY (userid, movieid));")


con.commit()
cur.close()
con.close()

""" def getOpenConnection(user='postgres', password='Boeing2018', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "'host='localhost' password'" + password + "'")

def loadRatings(ratingstablename, ratingsfilepath, openconnection):

    cur = openconnection.cursor()
    cur.execute("DROP TABLE IF EXISTS "+ratingstablename)
    cur.execute("CREATE TABLE "+ratingstablename+" (row_id serial primary key, UserID INT, temp1 VARCHAR(10),  MovieID INT , temp3 VARCHAR(10),  Rating FLOAT, temp5 VARCHAR(10), Timestamp INT)")
    
    loadout = open(ratingsfilepath,'r')
    
    cur.copy_from(loadout,ratingstablename,sep = ':',columns=('UserID','temp1','MovieID','temp3','Rating','temp5','Timestamp'))
    cur.execute("ALTER TABLE "+ratingstablename+" DROP COLUMN temp1, DROP COLUMN temp3,DROP COLUMN temp5, DROP COLUMN Timestamp")
    
    cur.close()

loadRatings(ratings,ratingsfilepath,'D:\Computer Science\Data Processing at Scale CSE 511\Assignments\Assignment 3 Data Fragmentation\Data Fragmentation\test_data.txt') """