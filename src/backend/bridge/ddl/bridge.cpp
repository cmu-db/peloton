//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// bridge.cpp
//
// Identification: src/backend/bridge/ddl/bridge.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <assert.h>
#include <sys/types.h>
#include <unistd.h>

#include "backend/bridge/ddl/bridge.h"
#include "backend/common/logger.h"

#include "postgres.h"
#include "c.h"
#include "miscadmin.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_class.h"
#include "catalog/pg_database.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "common/fe_memutils.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "parser/parse_type.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Getters
//===--------------------------------------------------------------------===//

/**
 * @brief Get the pg class tuple
 * @param tuple relevant tuple if it exists, NULL otherwise
 */
HeapTuple Bridge::GetPGClassTupleForRelationOid(Oid relation_id) {
  LOG_WARN("Do not use bridge function(%s) in Peloton !!! ", __func__);
  Relation pg_class_rel;
  HeapTuple tuple = NULL;

  // Open pg_class table
  pg_class_rel = heap_open(RelationRelationId, AccessShareLock);

  // Search the pg_class table with given relation id
  tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relation_id));
  if (!HeapTupleIsValid(tuple)) {
    elog(DEBUG2, "cache lookup failed for relation %u", relation_id);
    // Don't break here, we need to close heap and commit.
  }

  heap_close(pg_class_rel, AccessShareLock);

  return tuple;
}

/**
 * @brief Get the pg class tuple
 * @param tuple relevant tuple if it exists, NULL otherwise
 */
HeapTuple Bridge::GetPGClassTupleForRelationName(const char *relation_name) {
  LOG_WARN("Do not use bridge function(%s) in Peloton !!! ", __func__);
  Relation pg_class_rel;
  HeapTuple tuple = NULL;
  HeapScanDesc scan;

  // Open pg_class table
  pg_class_rel = heap_open(RelationRelationId, AccessShareLock);

  // Search the pg_class table with given relation name
  scan = heap_beginscan_catalog(pg_class_rel, 0, NULL);

  while (HeapTupleIsValid(tuple = heap_getnext(scan, ForwardScanDirection))) {
    Form_pg_class pgclass = (Form_pg_class)GETSTRUCT(tuple);

    if (pgclass->relnamespace == PG_PUBLIC_NAMESPACE) {
      if (strcmp(NameStr(pgclass->relname), relation_name) == 0) {
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
char *Bridge::GetRelationName(Oid relation_id) {
  LOG_WARN("Do not use bridge function(%s) in Peloton !!! ", __func__);
  HeapTuple tuple;
  Form_pg_class pg_class;
  char *relation_name;

  tuple = GetPGClassTupleForRelationOid(relation_id);
  if (!HeapTupleIsValid(tuple)) {
    return NULL;
  }

  // Get relation name
  pg_class = (Form_pg_class)GETSTRUCT(tuple);
  relation_name = NameStr(pg_class->relname);

  return relation_name;
}

/*
 * given a table name, look up the OID
 * @param table_name table name
 * @return relation id, if relation is valid, 0 otherewise
 */
Oid Bridge::GetRelationOid(const char *relation_name) {
  LOG_WARN("Do not use bridge function(%s) in Peloton !!! ", __func__);
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
  LOG_WARN("Do not use bridge function(%s) in Peloton !!! ", __func__);
  HeapTuple tuple;
  Form_pg_class pg_class;
  int num_atts = -1;

  tuple = GetPGClassTupleForRelationOid(relation_id);

  if (!HeapTupleIsValid(tuple)) {
    return num_atts;
  }

  pg_class = (Form_pg_class)GETSTRUCT(tuple);

  // Get number of attributes
  num_atts = pg_class->relnatts;

  return num_atts;
}

/**
 * @brief Getting the number of tuples.
 * @param relation_id relation id
 * @return num_tuples if valid relation_id, otherwise -1
 */
float Bridge::GetNumberOfTuples(Oid relation_id) {
  LOG_WARN("Do not use bridge function(%s) in Peloton !!! ", __func__);
  HeapTuple tuple;
  Form_pg_class pg_class;
  float num_tuples;

  tuple = GetPGClassTupleForRelationOid(relation_id);
  if (!HeapTupleIsValid(tuple)) {
    return -1;
  }
  pg_class = (Form_pg_class)GETSTRUCT(tuple);

  // Get number of tuples
  num_tuples = pg_class->reltuples;

  return num_tuples;
}

/**
 * @brief Getting the current database Oid
 * @return MyDatabaseId
 */
Oid Bridge::GetCurrentDatabaseOid(void) { return MyDatabaseId; }

/**
 * @Determine whether table exists in the *current* database or not
 * @param table_name table name
 * @return true or false depending on whether table exists or not.
 */
bool Bridge::RelationExists(const char *relation_name) {
  LOG_WARN("Do not use bridge function(%s) in Peloton !!! ", __func__);
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
  LOG_WARN("Do not use bridge function(%s) in Peloton !!! ", __func__);
  Relation pg_class_rel;
  HeapScanDesc scan;
  HeapTuple tuple;

  // Scan pg class table
  pg_class_rel = heap_open(RelationRelationId, AccessShareLock);
  scan = heap_beginscan_catalog(pg_class_rel, 0, NULL);

  while (HeapTupleIsValid(tuple = heap_getnext(scan, ForwardScanDirection))) {
    Form_pg_class pgclass = (Form_pg_class)GETSTRUCT(tuple);

    // Check if we only need catalog tables or not ?
    if (catalog_only == false) {
      elog(LOG, "pgclass->relname :: %s ", NameStr(pgclass->relname));
    } else if (pgclass->relnamespace == PG_PUBLIC_NAMESPACE) {
      elog(LOG, "pgclass->relname :: %s ", NameStr(pgclass->relname));
    }
  }

  heap_endscan(scan);
  heap_close(pg_class_rel, AccessShareLock);

}

/**
 * @brief Print all databases using catalog table pg_database
 */
void Bridge::GetDatabaseList(void) {
  LOG_WARN("Do not use bridge function(%s) in Peloton !!! ", __func__);
  Relation pg_database_rel;
  HeapScanDesc scan;
  HeapTuple tup;

  // Scan pg database table
  pg_database_rel = heap_open(DatabaseRelationId, AccessShareLock);
  scan = heap_beginscan_catalog(pg_database_rel, 0, NULL);

  while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection))) {
    Form_pg_database pg_database = (Form_pg_database)GETSTRUCT(tup);
    Oid database_oid = HeapTupleHeaderGetOid(tup->t_data);
    elog(LOG, "pgdatabase->datname  :: %s oid %d ",
         NameStr(pg_database->datname), (int)database_oid);
  }

  heap_endscan(scan);
  heap_close(pg_database_rel, AccessShareLock);
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
  assert(relation_id);

  Relation pg_class_rel;
  HeapTuple tuple;
  Form_pg_class pgclass;

  // Open target table in exclusive mode
  pg_class_rel = heap_open(RelationRelationId, RowExclusiveLock);
  tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relation_id));
  if (!HeapTupleIsValid(tuple)) {
    elog(DEBUG2, "cache lookup failed for relation %u", relation_id);
  } else {
    pgclass = (Form_pg_class)GETSTRUCT(tuple);
    pgclass->reltuples = (float4)num_tuples;
    pgclass->relpages = (int32)1;

    // update tuple
    simple_heap_update(pg_class_rel, &tuple->t_self, tuple);

    /* keep the catalog indexes up to date */
    CatalogUpdateIndexes(pg_class_rel, tuple);

    heap_freetuple(tuple);
  }

  heap_close(pg_class_rel, RowExclusiveLock);
}

//===--------------------------------------------------------------------===//
// Wrapper
//===--------------------------------------------------------------------===//

void Bridge::PelotonStartTransactionCommand() {
  LOG_WARN("Do not use bridge function(%s) in Peloton !!! ", __func__);
  StartTransactionCommand();
}

void Bridge::PelotonCommitTransactionCommand() {
  LOG_WARN("Do not use bridge function(%s) in Peloton !!! ", __func__);
  CommitTransactionCommand();
  SetCurrentResourceOwner();
}

void Bridge::SetCurrentResourceOwner() {
  LOG_WARN("Do not use bridge function(%s) in Peloton !!! ", __func__);
  // Set the resource owner
  CurrentResourceOwner = ResourceOwnerCreate(NULL, "Peloton");
}

}  // namespace bridge
}  // namespace peloton
