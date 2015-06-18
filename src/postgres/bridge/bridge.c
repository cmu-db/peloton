/*
 * bridge.c
 *
 *  Created on: Jun 11, 2015
 *      Author: Jinwoong Kim
 */

#include "postgres.h"

#include "../../bridge/bridge.h"

#include "access/heapam.h"
#include "access/xact.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h" // for RELOID

#include "c.h" // for NameStr 
#include "catalog/pg_database.h" // for  DatabaseRelationId, AccessShareLock
#include "catalog/pg_class.h" // for  RelationRelationId 
#include "access/htup_details.h" // for  GETSTRUCT
#include "common/fe_memutils.h" // for pg_strdup


/**
 * @brief Getting the relation name.
 * @param relation_id relation id
 */
char* 
GetRelationName(Oid relation_id) {
  Relation relation;
  char *relation_name;

  relation = relation_open(relation_id, NoLock);

  if(relation == NULL)
    return "";

  // Get name for given relation
  relation_name = quote_qualified_identifier(get_namespace_name(RelationGetNamespace(relation)), RelationGetRelationName(relation));

  // Simple version of above function
  // relation_name = RelationGetRelationName(relation);

  relation_close(relation, NoLock);

  return relation_name;
}

/**
 * @brief Getting the number of attributes.
 * @param relation_id relation id
 */
int 
GetNumberOfAttributes(Oid relation_id) {
  Relation relation;
  AttrNumber numOfAttris;

  relation = relation_open(relation_id, NoLock);

  //Get the number of attributes
  numOfAttris = RelationGetNumberOfAttributes(relation);

  relation_close(relation, NoLock);

  return numOfAttris;
}

/**
 * @brief Getting the number of tuples.
 * @param relation_id relation id
 */
float 
GetNumberOfTuples(Oid relation_id) {
  Relation HeapRelation;
  float  num_of_tuples;

  StartTransactionCommand();
  HeapRelation = relation_open(relation_id,NoLock/*must be know about lock..*/);
  //HeapRelation = heap_open(RelationRelationId,RowExclusiveLock/*must be know about lock..*/);

  //Get the number of tuples from pg_class
  num_of_tuples = HeapRelation->rd_rel->reltuples;

  heap_close(HeapRelation, NoLock);

  CommitTransactionCommand();
  return num_of_tuples;;
}

/**
 * @brief Setting the number of tuples.
 * @param relation_id relation id
 * @param num_of_tuples number of tuples
 */
void 
SetNumberOfTuples(Oid relation_id, float num_tuples) {
  Relation pg_class_rel;
  HeapTuple tuple;
  Form_pg_class pgcform;
  bool dirty;

  StartTransactionCommand();

  pg_class_rel = heap_open(RelationRelationId,RowExclusiveLock);

  tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relation_id));
  if (!HeapTupleIsValid(tuple))
    elog(ERROR, "cache lookup failed for relation %u", relation_id);

  pgcform = (Form_pg_class) GETSTRUCT(tuple);

  dirty = false;
  if (pgcform->reltuples != (float4) num_tuples)
  {
    pgcform->reltuples = (float4) num_tuples;
    dirty = true;
  }

  /* If anything changed, write out the tuple. */
  if (dirty)
    simple_heap_update(pg_class_rel, &tuple->t_data->t_ctid, tuple);
  //heap_inplace_update(pg_class_rel, tuple);

  heap_inplace_update(pg_class_rel, tuple);

  heap_close(pg_class_rel, RowExclusiveLock);

  CommitTransactionCommand();
}

/**
 * @brief Printing all databases from catalog table, i.e., pg_database
 */
void GetDatabaseList(void) {
  Relation	rel;
  HeapScanDesc scan;
  HeapTuple	tup;

  StartTransactionCommand();
  //(void) GetTransactionSnapshot();

  rel = heap_open(DatabaseRelationId, AccessShareLock);
  scan = heap_beginscan_catalog(rel, 0, NULL);

  while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
  {
    Form_pg_database pgdatabase = (Form_pg_database) GETSTRUCT(tup);

    printf(" pgdatabase->datname %s\n", NameStr(pgdatabase->datname) );
    printf(" pgdatabase->datdba %d\n", pgdatabase->datdba);
    printf(" pgdatabase->encoding %d\n", pgdatabase->encoding);
    //printf(" pgdatabase->encoding %s\n", encodingid_to_string(pgdatabase->encoding));
    printf(" pgdatabase->datcollate %s\n", NameStr(pgdatabase->datcollate));
    printf(" pgdatabase->datctype) %s\n", NameStr(pgdatabase->datctype));

    /*
               const char conninfo[50];
               PGconn     *conn;
	       sprintf(conninfo, "dbname = %s", NameStr(pgdatabase->datname));
	       conn = PQconnectdb(conninfo);

	       // Check to see that the backend connection was successfully made 
	       if (PQstatus(conn) != CONNECTION_OK)
	       {
		       printf("Connection to database failed: %s", PQerrorMessage(conn));
		       exit_nicely(conn);
	       }
     */

  }

  heap_endscan(scan);
  heap_close(rel, AccessShareLock);

  CommitTransactionCommand();
}

/**
 * @brief Printing all tables of current database from catalog table, i.e., pg_class
 */
void GetTableList(void) {
  Relation	rel;
  HeapScanDesc scan;
  HeapTuple	tup;

  StartTransactionCommand();
  //(void) GetTransactionSnapshot();

  rel = heap_open(RelationRelationId, AccessShareLock);
  scan = heap_beginscan_catalog(rel, 0, NULL);

  while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
  {
    Form_pg_class pgclass = (Form_pg_class) GETSTRUCT(tup);
    printf(" pgclass->datname %s, ", NameStr(pgclass->relname ) );
  }
  printf("\n");

  heap_endscan(scan);
  heap_close(rel, AccessShareLock);

  CommitTransactionCommand();

}


struct user_pg_database
{
  char datname[10];
  int datdba;
  int encoding;
};

typedef struct user_pg_database *Form_user_pg_database;

//Just testing whether I can modify table or not 
void Test1(Oid relation_id)
{
  Relation rel;
  HeapTuple	newtup;
  //HeapTuple	tup, newtup;
  Oid relid;
  Form_user_pg_database userpgdatabase;

  StartTransactionCommand();
  rel = heap_open(relation_id, RowExclusiveLock);
  relid = RelationGetRelid(rel);

  //	/* must be owner */
  //	if (!pg_database_ownercheck(db_id, GetUserId()))
  //		aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_DATABASE,
  //					   oldname);
  //
  //	/* must have createdb rights */
  //	if (!have_createdb_privilege())
  //		ereport(ERROR,
  //				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
  //				 errmsg("permission denied to rename database")));

  // ref. simple_heap_update

  /* fetch the tuple from system cache */
  newtup = SearchSysCacheCopy1(USERMAPPINGOID, ObjectIdGetDatum(relid));
  userpgdatabase = (Form_user_pg_database) GETSTRUCT(newtup);

  if (!HeapTupleIsValid(newtup))
    elog(ERROR, "cache lookup failed for the new tuple");

  printf("test11 %d \n", userpgdatabase->encoding );
  if( userpgdatabase->encoding == 101)
    userpgdatabase->encoding = 1001;
  printf("test12 %d \n", userpgdatabase->encoding );
  printf("%s %d\n", __func__, __LINE__);
  //namestrcpy( &(((Form_user_pg_database) GETSTRUCT(newtup))->datname), "testdb101");
  //way1
  /* update tuple */
  simple_heap_update(rel, &newtup->t_self, newtup);

  printf("%s %d\n", __func__, __LINE__);
  //way2
  /*
   * If anything changed, write out the tuple
   */
  //	if (dirty)
  //	{
  //		heap_inplace_update(pg_class, tuple);
  //		/* the above sends a cache inval message */
  //	}
  //	else
  //	{
  //		/* no need to change tuple, but force relcache inval anyway */
  //		CacheInvalidateRelcacheByTuple(tuple);
  //	}

  //CatalogUpdateIndexes(rel, newtup);

  //InvokeObjectPostAlterHook(DatabaseRelationId, db_id, 0);

  //ObjectAddressSet(address, DatabaseRelationId, db_id);

  //heap_freetuple(tup);
  heap_freetuple(newtup);
  /*
   * Close relation, but keep lock till commit.
   */
  heap_close(rel, RowExclusiveLock);
  CommitTransactionCommand();
}
