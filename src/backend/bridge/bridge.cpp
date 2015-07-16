/**
 * @brief Implementation of bridge.
 *
 * These utilities allow us to manage Postgres metadata.
 *
 * Copyright(c) 2015, CMU
 */

#include <iostream>
#include <sys/types.h>
#include <unistd.h>

#include "backend/bridge/bridge.h"

#include "postgres.h"
#include "c.h"
#include "miscadmin.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "bridge/bridge.h"
#include "backend/bridge/ddl.h"
#include "backend/catalog/schema.h"
#include "backend/catalog/constraint.h"
#include "backend/common/logger.h"
#include "backend/storage/database.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_class.h"
#include "catalog/pg_database.h"
#include "catalog/pg_namespace.h"
#include "common/fe_memutils.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Getters
//===--------------------------------------------------------------------===//

/**
 * @brief Get the pg class tuple
 * @param tuple relevant tuple if it exists, NULL otherwise
 */
HeapTuple Bridge::GetPGClassTupleForRelationOid(Oid relation_id){
  Relation pg_class_rel;
  HeapTuple tuple = NULL;

  StartTransactionCommand();

  // Open pg_class table
  pg_class_rel = heap_open(RelationRelationId, AccessShareLock);

  // Search the pg_class table with given relation id
  tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relation_id));
  if (!HeapTupleIsValid(tuple)) {
    elog(DEBUG2, "cache lookup failed for relation %u", relation_id);
    // Don't break here, we need to close heap and commit.
  }

  heap_close(pg_class_rel, AccessShareLock);
  CommitTransactionCommand();

  return tuple;
}

/**
 * @brief Get the pg class tuple
 * @param tuple relevant tuple if it exists, NULL otherwise
 */
HeapTuple Bridge::GetPGClassTupleForRelationName(const char *relation_name){
  Relation pg_class_rel;
  HeapTuple tuple = NULL;
  HeapScanDesc scan;

  // Open pg_class table
  pg_class_rel = heap_open(RelationRelationId, AccessShareLock);

  // Search the pg_class table with given relation name
  scan = heap_beginscan_catalog(pg_class_rel, 0, NULL);

  while (HeapTupleIsValid(tuple = heap_getnext(scan, ForwardScanDirection))) {
    Form_pg_class pgclass = (Form_pg_class) GETSTRUCT(tuple);

    if( pgclass->relnamespace==PG_PUBLIC_NAMESPACE){
      if(strcmp(NameStr(pgclass->relname), relation_name) == 0) {
        // We need to end scan and close heap
        break;
      }
    }
  }

  heap_endscan(scan);
  heap_close(pg_class_rel, AccessShareLock);

  return tuple;
}

/**
 * @brief Getting the relation name
 * @param relation_id relation id
 * @return Tuple if valid relation_id, otherwise null
 */
char* Bridge::GetRelationName(Oid relation_id){
  HeapTuple tuple;
  Form_pg_class pg_class;
  char* relation_name;

  tuple = GetPGClassTupleForRelationOid(relation_id);
  if (!HeapTupleIsValid(tuple)) {
    return NULL;
  }

  // Get relation name
  pg_class = (Form_pg_class) GETSTRUCT(tuple);
  relation_name = NameStr(pg_class->relname);

  return relation_name;
}

/*
 * given a table name, look up the OID
 * @param table_name table name
 * @return relation id, if relation is valid, 0 otherewise
 */
Oid Bridge::GetRelationOid(const char *relation_name){
  Oid relation_oid = InvalidOid;
  HeapTuple tuple;

  tuple = GetPGClassTupleForRelationName(relation_name);
  if (!HeapTupleIsValid(tuple)) {
    return InvalidOid;
  }

  // Get relation oid
  relation_oid = HeapTupleHeaderGetOid(tuple->t_data);

  return relation_oid;
}

//===--------------------------------------------------------------------===//
// Catalog information
//===--------------------------------------------------------------------===//

/**
 * @brief Getting the number of attributes.
 * @param relation_id relation id
 * @return num_atts if valid relation_id, otherwise -1
 */
int Bridge::GetNumberOfAttributes(Oid relation_id) {
  HeapTuple tuple;
  Form_pg_class pg_class;
  int num_atts = -1;

  tuple = GetPGClassTupleForRelationOid(relation_id);
  if (!HeapTupleIsValid(tuple)) {
    return num_atts;
  }

  pg_class = (Form_pg_class) GETSTRUCT(tuple);

  // Get number of attributes
  num_atts = pg_class->relnatts;

  return num_atts;
}

/**
 * @brief Getting the number of tuples.
 * @param relation_id relation id
 * @return num_tuples if valid relation_id, otherwise -1
 */
float Bridge::GetNumberOfTuples(Oid relation_id){
  HeapTuple tuple;
  Form_pg_class pg_class;
  float num_tuples;

  tuple = GetPGClassTupleForRelationOid(relation_id);
  if (!HeapTupleIsValid(tuple)) {
    return -1;
  }

  pg_class = (Form_pg_class) GETSTRUCT(tuple);

  // Get number of tuples
  num_tuples = pg_class->reltuples;

  return num_tuples;
}

/**
 * @brief Getting the current database Oid
 * @return MyDatabaseId
 */
Oid Bridge::GetCurrentDatabaseOid(void){
  return MyDatabaseId;
}


/**
 * @Determine whether table exists in the *current* database or not
 * @param table_name table name
 * @return true or false depending on whether table exists or not.
 */
bool Bridge::RelationExists(const char* relation_name) {
  HeapTuple tuple;

  tuple = GetPGClassTupleForRelationName(relation_name);
  if (!HeapTupleIsValid(tuple)) {
    return false;
  }

  return true;
}

//===--------------------------------------------------------------------===//
// Table lists
//===--------------------------------------------------------------------===//

/**
 * @brief Print all tables in *current* database using catalog table pg_class
 */
void Bridge::GetTableList(bool catalog_only) {
  Relation pg_class_rel;
  HeapScanDesc scan;
  HeapTuple tuple;

  //StartTransactionCommand();

  // Scan pg class table
  pg_class_rel = heap_open(RelationRelationId, AccessShareLock);
  scan = heap_beginscan_catalog(pg_class_rel, 0, NULL);

  while (HeapTupleIsValid(tuple = heap_getnext(scan, ForwardScanDirection))) {
    Form_pg_class pgclass = (Form_pg_class) GETSTRUCT(tuple);

    // Check if we only need catalog tables or not ?
    if(catalog_only == false) {
      elog(LOG, "pgclass->relname :: %s ", NameStr(pgclass->relname ) );
    }
    else if(pgclass->relnamespace==PG_PUBLIC_NAMESPACE) {
      elog(LOG, "pgclass->relname :: %s ", NameStr(pgclass->relname ) );
    }

  }

  heap_endscan(scan);
  heap_close(pg_class_rel, AccessShareLock);

  //CommitTransactionCommand();
}

/**
 * @brief Print all databases using catalog table pg_database
 */
void Bridge::GetDatabaseList(void) {
  Relation pg_database_rel;
  HeapScanDesc scan;
  HeapTuple tup;

  StartTransactionCommand();

  // Scan pg database table
  pg_database_rel = heap_open(DatabaseRelationId, AccessShareLock);
  scan = heap_beginscan_catalog(pg_database_rel, 0, NULL);

  while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))  {
    Form_pg_database pg_database = (Form_pg_database) GETSTRUCT(tup);
    elog(LOG, "pgdatabase->datname  :: %s ", NameStr(pg_database->datname) );
  }

  heap_endscan(scan);
  heap_close(pg_database_rel, AccessShareLock);

  CommitTransactionCommand();
}

//===--------------------------------------------------------------------===//
// Setters
//===--------------------------------------------------------------------===//

/**
 * @brief Setting the number of tuples.
 * @param relation_id relation id
 * @param num_tuples number of tuples
 */
void Bridge::SetNumberOfTuples(Oid relation_id, float num_tuples) {
  Relation pg_class_rel;
  HeapTuple tuple;
  Form_pg_class pgclass;

  StartTransactionCommand();

  // Open pg_class table in exclusive mode
  pg_class_rel = heap_open(RelationRelationId,RowExclusiveLock);

  tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relation_id));
  if (!HeapTupleIsValid(tuple)) {
    elog(DEBUG2, "cache lookup failed for relation %u", relation_id);
    return;
  }

  pgclass = (Form_pg_class) GETSTRUCT(tuple);
  pgclass->reltuples = (float4) num_tuples;
  // update tuple
  simple_heap_update(pg_class_rel, &tuple->t_data->t_ctid, tuple);

  heap_close(pg_class_rel, RowExclusiveLock);

  CommitTransactionCommand();
}

} // namespace bridge
} // namespace peloton



