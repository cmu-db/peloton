/*
 * bridge.c
 *
 *  Created on: Jun 11, 2015
 *      Author: Jinwoong Kim
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/xact.h"
#include "../../bridge/bridge.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"

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
    printf("JWKIM :: Please....\n");
    printf(" pgdatabase->datname %s\n", NameStr(pgdatabase->datname) );
    printf(" pgdatabase->datdba %d\n", pgdatabase->datdba);
    printf(" pgdatabase->encoding %d\n", pgdatabase->encoding);
    //printf(" pgdatabase->encoding %s\n", encodingid_to_string(pgdatabase->encoding));
    printf(" pgdatabase->datcollate %s\n", NameStr(pgdatabase->datcollate));
    printf(" pgdatabase->datctype) %s\n", NameStr(pgdatabase->datctype));
    printf(" pgdatabase->datistemplate %d\n", pgdatabase->datistemplate);
    printf(" pgdatabase->datallowconn %d\n", pgdatabase->datallowconn);
    printf(" pgdatabase->datconnlimit %d\n", pgdatabase->datconnlimit);
    printf(" pgdatabase->datlastsysoid %d\n", pgdatabase->datlastsysoid);
    printf(" pgdatabase->datfrozenxid %d\n", pgdatabase->datfrozenxid);
    printf(" pgdatabase->datminmxid %d\n", pgdatabase->datminmxid);
    printf(" pgdatabase->dattablespace %d\n", pgdatabase->dattablespace);
    //printf(" pgdatabase->datacl %d\n", pgdatabase->datacl);

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
