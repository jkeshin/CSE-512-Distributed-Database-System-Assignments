#
# Assignment3 Interface
# @author: Keshin Jani
#

import psycopg2
import os
import sys
import threading

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.
    cur = openconnection.cursor()
    cur.execute("Drop table if exists " + OutputTable + ";")
    cur.execute("Create table " + OutputTable + " (like " + InputTable + " including all);")
    maxQuery = "select Max(" + SortingColumnName + ") from " + InputTable + ";"
    minQuery = "select Min(" + SortingColumnName + ") from " + InputTable + ";"
    cur.execute(maxQuery)
    high = cur.fetchone()[0]
    cur.execute(minQuery)
    low = cur.fetchone()[0]

    r = (float(high) - float(low)) / 5.0
    threads = [0, 0, 0, 0, 0]
    for i in range(5):
        min = low + i * r
        threads[i] = threading.Thread(target=parallel_sort, args=(InputTable, SortingColumnName, i, min, min + r, openconnection))
        threads[i].start()

    i = 0
    for thread in threads:
        thread.join()
        cur.execute("Insert into " + OutputTable + " select * from tempTable" + str(i) + ";")
        i = i + 1

    cur.close()
    openconnection.commit()

def parallel_sort (InputTable, SortingColumnName, i, min, max, openconnection):
    cur = openconnection.cursor()
    cur.execute("Drop table if exists tempTable" + str(i) + " ;")
    cur.execute("Create table tempTable" + str(i) + " (like " + InputTable + " including all);")
    if(i == 0):
        sortQuery1 = "Insert into tempTable" + str(i) + " select * from " + InputTable + " where " + SortingColumnName + " >= " + str(min) + " and " + SortingColumnName + " <= " + str(max) + " order by " + SortingColumnName + " ASC;"
        cur.execute(sortQuery1)
    else:
        sortQuery2 = "Insert into tempTable" + str(i) + " select * from " + InputTable + " where " + SortingColumnName + " > " + str(min) + " and " + SortingColumnName + " <= " + str(max) + " order by " + SortingColumnName + " ASC;"
        cur.execute(sortQuery2)
    return


def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    cur = openconnection.cursor()
    cur.execute("Drop table if exists " + OutputTable + ";")
    maxQuery1 = "select max(" + Table1JoinColumn + ") from " + InputTable1 + ";"
    minQuery1 = "select min(" + Table1JoinColumn + ") from " + InputTable1 + ";"

    maxQuery2 = "select max(" + Table2JoinColumn + ") from " + InputTable2 + ";"
    minQuery2 = "select min(" + Table2JoinColumn + ") from " + InputTable2 + ";"

    cur.execute(maxQuery1)
    high1 = cur.fetchone()[0]
    cur.execute(minQuery1)
    low1 = cur.fetchone()[0]

    cur.execute(maxQuery2)
    high2 = cur.fetchone()[0]
    cur.execute(minQuery2)
    low2 = cur.fetchone()[0]

    high = 0
    low = 0

    if (high1 >= high2):
        high = high1
    else:
        high = high2

    if (low1 <= low2):
        low = low1
    else:
        low = low2

    r = (high - low) / 5.0
    threads = [0, 0, 0, 0, 0]

    for i in range(5):
        min = low + i * r

        threads[i] = threading.Thread(target=parallel_join, args=(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, min, min + r, i, openconnection))
        threads[i].start()

    for thread in threads:
        thread.join()

    cur.execute("Create table " + OutputTable + " as select * from " + InputTable1 + " inner join " + InputTable2 + " on " + InputTable1 + "." + Table1JoinColumn + " = " + InputTable2 + "." + Table2JoinColumn + " where 1 = 2;")
    i = 0
    for i in range(5):
        cur.execute("Insert into " + OutputTable + " select * from outputCopy" + str(i) + ";")

    cur.close()
    openconnection.commit()


def parallel_join(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, min, max, i, openconnection):
    cur = openconnection.cursor()
    cur.execute("Drop table if exists outputCopy" + str(i) + ";")
    cur.execute("Drop table if exists t_table1_" + str(i) + ";")
    cur.execute("Drop table if exists t_table2_" + str(i) + ";")
    cur.execute("Create table t_table1_" + str(i) + " (like " + InputTable1 + ");")
    cur.execute("Create table t_table2_" + str(i) + " (like " + InputTable2 + ");")

    if (i == 0):
        cur.execute("create table outputCopy" + str(i) + " as select * from " + InputTable1 + " inner join " + InputTable2 + " on " + InputTable1 + "." + Table1JoinColumn + " = " + InputTable2 + "." + Table2JoinColumn + " where 1 = 2;")
        cur.execute("insert into t_table1_" + str(i) + " select * from " + InputTable1 + " where " + Table1JoinColumn + " >= " + str(min) + " and " + Table1JoinColumn + " <= " + str(max) + ";")
        cur.execute("insert into t_table2_" + str(i) + " select * from " + InputTable2 + " where " + Table2JoinColumn + " >= " + str(min) + " and " + Table2JoinColumn + " <= " + str(max) + ";")
    else:
        cur.execute("create table outputCopy" + str(i) + " as select * from " + InputTable1 + " inner join " + InputTable2 + " on " + InputTable1 + "." + Table1JoinColumn + " = " + InputTable2 + "." + Table2JoinColumn + " where 1 = 2;")
        cur.execute("insert into t_table1_" + str(i) + " select * from " + InputTable1 + " where " + Table1JoinColumn + " > " + str(min) + " and " + Table1JoinColumn + " <= " + str(max) + ";")
        cur.execute("insert into t_table2_" + str(i) + " select * from " + InputTable2 + " where " + Table2JoinColumn + " > " + str(min) + " and " + Table2JoinColumn + " <= " + str(max) + ";")

    cur.execute("insert into outputCopy" + str(i) + " select * from t_table1_" + str(i) + " inner join t_table2_" + str(i) + " on t_table1_" + str(i) + "." + Table1JoinColumn + " = t_table2_" + str(i) + "." + Table2JoinColumn + ";")


################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
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
    con.commit()
    con.close()

# Donot change this function
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
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


