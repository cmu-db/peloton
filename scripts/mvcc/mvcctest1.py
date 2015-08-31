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
## This test assumes no index on the table
############################################

conn_string = "host='localhost' port='57721' dbname='tpcc' user='postgres' password='postgres'"
test_table = "foo"

drop_stmt = "DROP table if exists %s" % (test_table)
create_stmt = "CREATE table %s (a integer, b integer)" % (test_table)
select_stmt = "SELECT * from %s" % (test_table)
select1_stmt = "SELECT * from %s where a = %%s" % (test_table)
insert_stmt = "INSERT into %s values (%%s, %%s)" % (test_table)
delete_stmt = "DELETE from %s where a = %%s" % (test_table)
update_stmt = "UPDATE %s set b = %%s where a = %%s" % (test_table)

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

    cursor.execute(select_stmt)
    ##    records = cursor.fetchall()
    ##    print "Initial state: (%d tuples)"%(len(records))
    ##    pprint.pprint(records)

    conn.close()


def ReadMyInsertTest():
    RecreateTestTable()

    conn = psycopg2.connect(conn_string)
    conn.autocommit = False
    cursor = conn.cursor()

    # Confirm initial state
    cursor.execute(select_stmt)
    assert 2 == len(cursor.fetchall())

    # Do a Dirty insert
    cursor.execute(insert_stmt, (3, 33))
    # Let's see our dirty insert
    cursor.execute(select_stmt)
    assert 3 == len(cursor.fetchall())

    # Rollback
    conn.rollback()

    cursor.execute(select_stmt)
    assert 2 == len(cursor.fetchall())

    conn.close()


def DeleteMyInsertTest():
    RecreateTestTable()

    conn1 = psycopg2.connect(conn_string)
    conn1.autocommit = False
    term1 = conn1.cursor()

    # Confirm initial state
    term1.execute(select_stmt)
    assert 2 == len(term1.fetchall())

    # T1 do a Dirty insert (3)
    term1.execute(insert_stmt, (3, 33))
    # Let's see our dirty insert
    term1.execute(select_stmt)
    assert 3 == len(term1.fetchall())


    # T1 deletes (3)
    term1.execute(delete_stmt, (3,))
    # T1 should not see it
    term1.execute(select_stmt)
    assert 2 == len(term1.fetchall())

    # commit
    conn1.commit()

    # still 2 tuples
    term1.execute(select_stmt)
    assert 2 == len(term1.fetchall())


def UpdateMyInsert():
    RecreateTestTable()

    conn1 = psycopg2.connect(conn_string)
    conn1.autocommit = False
    term1 = conn1.cursor()

    # Confirm initial state
    term1.execute(select_stmt)
    assert 2 == len(term1.fetchall())

    # T1 do a Dirty insert (3)
    term1.execute(insert_stmt, (3, 33))
    # T1 see own insert
    term1.execute(select_stmt)
    assert 3 == len(term1.fetchall())

    # T1 updates (3)
    term1.execute(update_stmt, (300, 3))

    # T1 see own insert is updated
    term1.execute(select1_stmt, (3,))
    records = term1.fetchall()
    assert 1 == len(records)
    assert (3, 300) == records[0]

    # T1 commit
    conn1.commit()
    # T1 sees the final updated value
    term1.execute(select1_stmt, (3,))
    records = term1.fetchall()
    assert 1 == len(records)
    assert (3, 300) == records[0]

    conn1.close()


def ReadOtherInsertTest():
    RecreateTestTable()

    conn1 = psycopg2.connect(conn_string)
    conn1.autocommit = False
    term1 = conn1.cursor()

    conn2 = psycopg2.connect(conn_string)
    conn2.autocommit = False
    term2 = conn2.cursor()

    # Confirm initial state
    term1.execute(select_stmt)
    assert 2 == len(term1.fetchall())
    term2.execute(select_stmt)
    assert 2 == len(term2.fetchall())

    # T1 do a Dirty insert
    term1.execute(insert_stmt, (3, 33))
    # Let's see our dirty insert
    term1.execute(select_stmt)
    assert 3 == len(term1.fetchall())

    # T2 should not see it
    term2.execute(select_stmt)
    assert 2 == len(term2.fetchall())

    # T1 commit
    conn1.commit()
    # T1 sees the commited insert
    term1.execute(select_stmt)
    assert 3 == len(term1.fetchall())

    # T2 still shoud not see it
    term2.execute(select_stmt)
    assert 2 == len(term2.fetchall())

    # T2 starts a new txn
    conn2.commit()

    # T2 should see it now
    term2.execute(select_stmt)
    assert 3 == len(term2.fetchall())

    conn1.close()
    conn2.close()


def DeleteOtherDeleteTest():
    RecreateTestTable()

    conn1 = psycopg2.connect(conn_string)
    conn1.autocommit = False
    term1 = conn1.cursor()

    conn2 = psycopg2.connect(conn_string)
    conn2.autocommit = False
    term2 = conn2.cursor()

    # Confirm initial state
    term1.execute(select_stmt)
    assert 2 == len(term1.fetchall())
    term2.execute(select_stmt)
    assert 2 == len(term2.fetchall())

    # T1 deletes (1)
    term1.execute(delete_stmt, (1,))
    # T1 not see it
    term1.execute(select_stmt)
    assert 1 == len(term1.fetchall())

    # T2 should still see it
    term2.execute(select_stmt)
    assert 2 == len(term2.fetchall())

    # T2 tries to delete the same tuple, should fail
    catch = False
    try:
        term2.execute(delete_stmt, (1,))
    except:
        catch = True
    assert True == catch

    # T1 commits
    conn1.commit()

    # T2 should see it gone (in a new txn)
    conn2.rollback()
    term2.execute(select_stmt)
    assert 1 == len(term2.fetchall())

    conn1.close()
    conn2.close()


def DeleteOtherAbortedDeleteTest():
    RecreateTestTable()

    conn1 = psycopg2.connect(conn_string)
    conn1.autocommit = False
    term1 = conn1.cursor()

    conn2 = psycopg2.connect(conn_string)
    conn2.autocommit = False
    term2 = conn2.cursor()

    # Confirm initial state
    term1.execute(select_stmt)
    assert 2 == len(term1.fetchall())
    term2.execute(select_stmt)
    assert 2 == len(term2.fetchall())

    # T1 deletes (1)
    term1.execute(delete_stmt, (1,))
    # T1 not see it
    term1.execute(select_stmt)
    assert 1 == len(term1.fetchall())

    # T2 should still see it
    term2.execute(select_stmt)
    assert 2 == len(term2.fetchall())

    # T1 rollback
    conn1.rollback()

    # T2 tries to delete the same tuple
    term2.execute(delete_stmt, (1,))

    # T2 should see it gone (in a new txn)
    conn2.commit()
    term2.execute(select_stmt)
    assert 1 == len(term2.fetchall())

    conn1.close()
    conn2.close()


def UpdateOtherDeleteTest():
    RecreateTestTable()

    conn1 = psycopg2.connect(conn_string)
    conn1.autocommit = False
    term1 = conn1.cursor()

    conn2 = psycopg2.connect(conn_string)
    conn2.autocommit = False
    term2 = conn2.cursor()

    # Confirm initial state
    term1.execute(select_stmt)
    assert 2 == len(term1.fetchall())
    term2.execute(select_stmt)
    assert 2 == len(term2.fetchall())

    # T1 deletes (1)
    term1.execute(delete_stmt, (1,))
    # T1 not see it
    term1.execute(select_stmt)
    assert 1 == len(term1.fetchall())

    # T2 should still see it
    term2.execute(select_stmt)
    assert 2 == len(term2.fetchall())

    # T2 tries to update the same tuple, should fail
    catch = False
    try:
        term2.execute(update_stmt, (100, 1))
    except:
        catch = True
    assert True == catch

    # T1 commits
    conn1.commit()

    # T2 should see it gone (in a new txn)
    conn2.rollback()
    term2.execute(select_stmt)
    assert 1 == len(term2.fetchall())

    conn1.close()
    conn2.close()


def UpdateOtherUpdateTest():
    RecreateTestTable()

    conn1 = psycopg2.connect(conn_string)
    conn1.autocommit = False
    term1 = conn1.cursor(cursor_factory=psycopg2.extras.DictCursor)

    conn2 = psycopg2.connect(conn_string)
    conn2.autocommit = False
    term2 = conn2.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Confirm initial state
    term1.execute(select_stmt)
    assert 2 == len(term1.fetchall())
    term2.execute(select_stmt)
    assert 2 == len(term2.fetchall())

    # T1 updates (1)
    term1.execute(update_stmt, (100, 1))
    # T1 sees updated value
    term1.execute(select1_stmt, (1,))
    records = term1.fetchall()
    assert 1 == len(records)
    assert [1, 100] == records[0]

    # T2 should still see the old value
    term2.execute(select1_stmt, (1,))
    records = term2.fetchall()
    assert [1, 11] == records[0]

    # T2 tries to update the same tuple, should fail
    catch = False
    try:
        term2.execute(update_stmt, (200, 1))
    except:
        catch = True
    assert True == catch

    # T1 commits
    conn1.commit()

    # T2 should see updated value (in a new txn)
    conn2.rollback()
    term2.execute(select1_stmt, (1,))
    records = term2.fetchall()
    assert 1 == len(records)
    assert [1, 100] == records[0]

    conn1.close()
    conn2.close()


def UpdateOtherAbortedUpdateTest():
    RecreateTestTable()

    conn1 = psycopg2.connect(conn_string)
    conn1.autocommit = False
    term1 = conn1.cursor()

    conn2 = psycopg2.connect(conn_string)
    conn2.autocommit = False
    term2 = conn2.cursor()

    # Confirm initial state
    term1.execute(select_stmt)
    assert 2 == len(term1.fetchall())
    term2.execute(select_stmt)
    assert 2 == len(term2.fetchall())

    # T1 updates (1)
    term1.execute(update_stmt, (100, 1))
    # T1 sees updated value
    term1.execute(select1_stmt, (1,))
    records = term1.fetchall()
    assert 1 == len(records)
    assert (1, 100) == records[0]

    # T2 should still see the old value
    term2.execute(select1_stmt, (1,))
    records = term2.fetchall()
    assert (1, 11) == records[0]

    # T1 rollback
    conn1.rollback()

    # T2 tries to update the same
    term2.execute(update_stmt, (200, 1))

    # T2 should see updated value (in a new txn)
    conn2.commit()
    term2.execute(select1_stmt, (1,))
    records = term2.fetchall()
    assert 1 == len(records)
    ##    pprint.pprint(records)
    assert (1, 200) == records[0]

    conn1.close()
    conn2.close()

def UpdateSameTupleTest():
    RecreateTestTable()

    conn1 = psycopg2.connect(conn_string)
    conn1.autocommit = False
    term1 = conn1.cursor()

    conn2 = psycopg2.connect(conn_string)
    conn2.autocommit = False
    term2 = conn2.cursor()

    # Confirm initial state
    term1.execute(select_stmt)
    assert 2 == len(term1.fetchall())
    term2.execute(select_stmt)
    assert 2 == len(term2.fetchall())

    # T1 updates (1)
    term1.execute(update_stmt, (100, 1))
    # T1 sees updated value
    term1.execute(select1_stmt, (1,))
    records = term1.fetchall()
    assert 1 == len(records)
    assert (1, 100) == records[0]

    # T2 should still see the old value
    term2.execute(select1_stmt, (1,))
    records = term2.fetchall()
    assert (1, 11) == records[0]

    # T2 tries to update the same, should fail
    catch = False
    try:
        term2.execute(update_stmt, (200, 1))
    except Exception as e:
        catch = True

    assert catch is True

    # rollbacks
    conn2.rollback()

    # Commit
    conn1.commit()

    # T1 T2 should see the new values
    term2.execute(select1_stmt, (1,))
    records = term2.fetchall()
    assert (1, 100) == records[0]
    assert 1 == len(records)

    term1.execute(select1_stmt, (1,))
    records = term1.fetchall()
    assert (1, 100) == records[0]
    assert 1 == len(records)

    conn1.close()
    conn2.close()

def main():
    print "ReadMyInsertTest"
    ReadMyInsertTest()

    print "DeleteMyInsertTest"
    DeleteMyInsertTest()

    print "UpdateMyInsert"
    UpdateMyInsert()

    print "ReadOtherInsertTest"
    ReadOtherInsertTest()

    print "DeleteOtherDeleteTest"
    DeleteOtherDeleteTest()

    print "DeleteOtherAbortedDeleteTest"
    DeleteOtherAbortedDeleteTest()

    print "UpdateOtherDeleteTest"
    UpdateOtherDeleteTest()

    print "UpdateOtherUpdateTest"
    UpdateOtherUpdateTest()

    print "UpdateOtherAbortedUpdateTest"
    UpdateOtherAbortedUpdateTest()

    print "UpdateSameTupleTest"
    UpdateSameTupleTest()

    print "All tests are run."


if __name__ == "__main__":
    main()
