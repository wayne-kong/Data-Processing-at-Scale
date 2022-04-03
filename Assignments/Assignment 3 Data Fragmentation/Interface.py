#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()
    cur.execute("DROP TABLE IF EXISTS "+ratingstablename)
    cur.execute("CREATE TABLE "+ratingstablename+" (UserID INT, temp1 VARCHAR(10),  MovieID INT , temp3 VARCHAR(10),  Rating FLOAT, temp5 VARCHAR(10), Timestamp INT)")
    
    loadout = open(ratingsfilepath,'r')
    
    cur.copy_from(loadout,ratingstablename,sep = ':',columns=('UserID','temp1','MovieID','temp3','Rating','temp5','Timestamp'))
    cur.execute("ALTER TABLE "+ratingstablename+" DROP COLUMN temp1, DROP COLUMN temp3,DROP COLUMN temp5, DROP COLUMN Timestamp")
    
    cur.close()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    global range_condition  
    global x
    x = int(numberofpartitions)
    range_condition = float(5.0 / numberofpartitions)

    for i in range(0, x):
        if i == 0:
            j = float(i)
            cur.execute("DROP TABLE IF EXISTS range_part" + str(i) + ";")
            cur.execute(
                "CREATE TABLE range_part" + str(i) + " AS SELECT * FROM " + ratingstablename + " WHERE Rating >=" + str(
                    j * range_condition) +
                " AND Rating <=" + str((j + 1) * range_condition) + " ;")
        else:
            j = float(i)
            cur.execute("DROP TABLE IF EXISTS range_part" + str(i) + ";")
            cur.execute(
                "CREATE TABLE range_part" + str(i) + " AS SELECT * FROM " + ratingstablename + " WHERE Rating >" + str(
                    j * range_condition) +
                " AND Rating <=" + str((j + 1) * range_condition) + " ;")

    cur.close()


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    name = "rrobin_part"
    global nofp
    nofp = numberofpartitions
    global last_insert_position
    last_insert_position=0
    global roundrobinpartitions
    roundrobinpartitions = numberofpartitions
    

    cur = openconnection.cursor()


    cur.execute("SELECT * FROM %s" % ratingstablename)
    rows = cur.fetchall()

    tableNum = 0
    while tableNum < numberofpartitions:
        newTableName = name + str(tableNum)
        cur.execute("CREATE TABLE IF NOT EXISTS %s(UserID INT, MovieID INT, Rating FLOAT)" % (newTableName))
        tableNum += 1;

    lastInserted = 0
    for row in rows:
        newTableName = name + str(lastInserted)
        cur.execute("INSERT INTO %s(UserID, MovieID, Rating) VALUES(%d, %d, %f)" % (newTableName, row[0], row[1], row[2]))
        lastInserted = (lastInserted + 1) % numberofpartitions

    global lastroundrobintable
    lastroundrobintable = lastInserted
    openconnection.commit()
    
    cur.close()
    
                


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
   cur=openconnection.cursor()
   global last_insert_position
   global nofp
   p_end = last_insert_position % nofp
   cur.execute("INSERT INTO rrobin_part" +str(p_end)+ " (UserID,MovieID,Rating) VALUES (" +str(userid)+ "," +str(
            itemid)+ "," +str(rating)+ ");")
   cur.close()



def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    cur=openconnection.cursor()
    new_range=5.0/x

    low_bound=0
    cur_partition=0
    up_bound=new_range

    while low_bound<5:
        if low_bound ==0:
            if rating >= low_bound and rating <= up_bound:
                break
            cur_partition=cur_partition+1
            low_bound=low_bound+new_range
            up_bound=up_bound+new_range

        else:
            if rating > low_bound and rating <= up_bound:
                break
            cur_partition = cur_partition + 1
            low_bound = low_bound + new_range
            up_bound = up_bound + new_range

    cur.execute("INSERT INTO range_part" +str(cur_partition)+ "(UserID, MovieID, rating) VALUES ("  + str(userid) + "," + str(
            itemid) + "," + str(rating) + ")")

    cur.close()

def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    finally:
        if cursor:
            cursor.close()
