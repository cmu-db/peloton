#!/usr/bin/env python
# encoding: utf-8

import psycopg2
import psycopg2.extras

############################################
## NOTE:
## psychopg will issue a 'BEGIN' by default
## right after you create a connection
## or call commit()/rollback()
############################################

############################################
## This test assumes primary key on column 'a'
############################################

conn_string = "host='localhost' port='57721' dbname='tpcc' user='postgres' password='postgres'"
test_table = "foo"

drop_stmt = "DROP table if exists %s" % (test_table)
create_stmt = "CREATE table %s (a integer PRIMARY KEY, b integer)" % (test_table)
select_stmt = "SELECT * from %s" % (test_table)
select1_stmt = "SELECT * from %s where a = %%s" % (test_table)
select2_stmt = "SELECT * from %s where b = %%s" % (test_table)
insert_stmt = "INSERT into %s values (%%s, %%s)" % (test_table)
delete_stmt = "DELETE from %s where a = %%s" % (test_table)
update_stmt = "UPDATE %s set b = %%s where a = %%s" % (test_table)
update1_stmt = "UPDATE %s set a = %%s where b = %%s" % (test_table)

### Recreate the test table with initial values
### |a, b |
### (1, 11)
### (2, 22)
def RecreateTestTable():
    ##    print "Connecting to:\n %s\n" % (conn_string)

    try:
        conn = psycopg2.connect(conn_string)
    except Exception as e:
        print e

    conn.autocommit = True
    cursor = conn.cursor()

    cursor.execute(drop_stmt)
    cursor.execute(create_stmt)

    cursor.execute(insert_stmt, (1, 11))
    cursor.execute(insert_stmt, (2, 22))

    ##    cursor.execute(select_stmt)
    ##    records = cursor.fetchall()
    ##    print "Initial state: (%d tuples)"%(len(records))
    ##    pprint.pprint(records)

    conn.close()


def InsertExistingKeyTest():
    RecreateTestTable()

    conn1 = psycopg2.connect(conn_string)
    term1 = conn1.cursor()

    # T1 tries to insert an existing key (2), should fail
    catch = False
    try:
        term1.execute(insert_stmt, (2, 200))
    except:
        catch = True
    assert True == catch

    conn1.close()


def InsertSameKeyTest():
    RecreateTestTable()

    conn1 = psycopg2.connect(conn_string)
    term1 = conn1.cursor()

    # T1 tries to insert (3, 33)
    term1.execute(insert_stmt, (3, 33))

    # T1 insert (3, 33) again
    catch = False
    try:
        term1.execute(insert_stmt, (3, 33))
    except:
        catch = True
    assert True == catch

    conn1.close()


def DeleteThenInsertKeyTest():
    RecreateTestTable()

    conn1 = psycopg2.connect(conn_string)
    term1 = conn1.cursor()

    # T1 deletes key (2)
    term1.execute(delete_stmt, (2,))
    # T1 selects (2), no result
    term1.execute(select1_stmt, (2,))
    records = term1.fetchall()
    assert 0 == len(records)

    # T1 inserts key (2)
    term1.execute(insert_stmt, (2, 200))
    # T1 select (2), see new tuple
    term1.execute(select1_stmt, (2,))
    records = term1.fetchall()
    assert 1 == len(records)
    assert (2, 200) == records[0]


def AbortOverrideKeyTest():
    RecreateTestTable()

    conn1 = psycopg2.connect(conn_string)
    term1 = conn1.cursor()

    # T1 deletes key (2)
    term1.execute(delete_stmt, (2,))

    # T1 inserts key (2)
    term1.execute(insert_stmt, (2, 200))
    # T1 select (2), see new tuple
    term1.execute(select1_stmt, (2,))
    records = term1.fetchall()
    assert 1 == len(records)
    assert (2, 200) == records[0]

    # T1 rollback
    conn1.rollback()

    # T1 should see old value (new txn)
    term1.execute(select1_stmt, (2,))
    records = term1.fetchall()
    assert 1 == len(records)
    assert (2, 22) == records[0]

    conn1.close()


def UpdateChangeKeyTest():
    RecreateTestTable()

    conn1 = psycopg2.connect(conn_string)
    term1 = conn1.cursor()

    # T1 update (2, 22) to (200, 22)
    term1.execute(update1_stmt, (200, 22))
    # T1 select b=22
    term1.execute(select2_stmt, (22,))
    records = term1.fetchall()
    assert 1 == len(records)
    assert (200, 22) == records[0]

    # T1 insert (2, 400)
    term1.execute(insert_stmt, (2, 400))
    # T1 select a=2
    term1.execute(select1_stmt, (2,))
    records = term1.fetchall()
    assert 1 == len(records)
    assert (2, 400) == records[0]

    conn1.close()


def main():
    print "InsertExistingKeyTest"
    InsertExistingKeyTest()

    print "InsertSameKeyTest"
    InsertSameKeyTest()

    print "DeleteThenInsertKeyTest"
    DeleteThenInsertKeyTest()

    print "AbortOverrideKeyTest"
    AbortOverrideKeyTest()

    print "UpdateChangeKeyTest"
    UpdateChangeKeyTest()


if __name__ == "__main__":
    main()
