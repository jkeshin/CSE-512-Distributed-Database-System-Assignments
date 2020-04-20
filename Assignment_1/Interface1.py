import psycopg2

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()
    cur.execute('Create table if not exists ratings( UserId integer, MovieId integer, Rating decimal)')

    dataFile = open(ratingsfilepath, 'r')
    for line in dataFile:
        val = line.split('::')
        del val[3:]
        cur.execute("INSERT INTO " + ratingstablename + " VALUES (%s, %s, %s)", val)
    cur.close()
    openconnection.commit()

    pass


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    partitionVal = 5 / numberofpartitions
    for i in range(numberofpartitions):
        partitionName = "range_part" + str(i)
        cur.execute("DROP TABLE IF EXISTS "+ partitionName)
        cur.execute("create table if not exists " + partitionName + " ( UserId integer, MovieId integer, Rating decimal)")
    #Metadata insert
    cur.execute("drop table if exists range_metadata")
    cur.execute("create table if not exists range_metadata (ptype varchar(20) , partition_no integer)")
    metadata_val = ('range_partition', numberofpartitions)
    cur.execute("insert into range_metadata values ( %s, %s)", metadata_val)

    for i in range(numberofpartitions):
        partitionName = "range_part" + str(i)
        if(i == 0):
            insertsql = "insert into " + partitionName + " select * from " + ratingstablename + " where rating >= " + str(i * partitionVal) + " and rating <=" + str((i + 1) * partitionVal)
        else:
            insertsql = "insert into " + partitionName + " select * from " + ratingstablename + " where rating > " + str(i*partitionVal) + " and rating <=" + str((i+1)*partitionVal)
        cur.execute(insertsql)

    cur.close()
    openconnection.commit()

    pass


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    for i in range(numberofpartitions):
        partitionName = "rrobin_part" + str(i)
        cur.execute("DROP TABLE IF EXISTS " + partitionName)
        cur.execute("create table if not exists " + partitionName + " ( UserId integer, MovieId integer, Rating decimal)")
    #Metadata Insert
    cur.execute("drop table if exists rrobin_metadata")
    cur.execute("create table if not exists rrobin_metadata (ptype varchar(20) , partition_no integer)")
    metadata_val = ('rrobin_partition', numberofpartitions)
    cur.execute("insert into rrobin_metadata values ( %s, %s)", metadata_val)

    cur.execute("select * from " + ratingstablename)
    all_rcds = cur.fetchall()
    i = 0
    for rcd in all_rcds:
        val = i % numberofpartitions
        i = i+1
        partitionName = "rrobin_part" + str(val)
        #print(rcd)
        cur.execute("insert into " + partitionName + " (Userid, Movieid, Rating ) VALUES (%s, %s, %s)", rcd  )

    cur.close()
    openconnection.commit()
    pass


def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()

    cur.execute("select count(*) from ratings")
    count = cur.fetchone()[0]
    cur.execute("select partition_no from rrobin_metadata")
    partition_no = cur.fetchone()[0]
    partitionName = "rrobin_part" + str((count)%partition_no)
    #print(partitionName)
    insertsql = "insert into "+ partitionName + " values (%s, %s, %s)"
    # print(insertsql)
    cur.execute(insertsql, (userid, itemid, rating))

    cur.close()
    openconnection.commit()
    pass


def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()

    cur.execute("select partition_no from range_metadata")
    partition_no = cur.fetchone()[0]
    for i in range(partition_no):
        if(i == 0 and rating >= i and rating <= 5/partition_no):
            cur.execute("insert into range_part" + str(i) + " values (%s, %s, %s)", (userid, itemid, rating))
        elif( rating > i * 5/partition_no and rating <= (i+1)*(5/partition_no) ):
            cur.execute("insert into range_part" + str(i) + " values (%s, %s, %s)", (userid, itemid, rating))

    cur.close()
    openconnection.commit()
    pass

def createDB(dbname='dds_assignment1'):
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
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

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
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()

if __name__ == "__main__":
    createDB()
    con = getOpenConnection(dbname='dds_assignment1')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    loadRatings("ratings", "test_data1.txt",con )
    roundRobinPartition("ratings", 6, con)
    roundRobinInsert('ratings', 2, 3, 3, con)
    rangePartition("ratings", 4, con)
    rangeInsert('ratings', 2, 3, 3, con)
