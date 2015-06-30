/**
 * @brief Implementation of bridge.
 *
 * These utilities allow us to manage Postgres metadata.
 *
 * Copyright(c) 2015, CMU
 */

#include "postgres.h"
#include "c.h"

#include "miscadmin.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "bridge/bridge.h"
#include "backend/bridge/ddl.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_database.h"
#include "catalog/pg_class.h"
#include "catalog/pg_namespace.h"
#include "common/fe_memutils.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#include <sys/types.h>
#include <unistd.h>

//===--------------------------------------------------------------------===//
// Postgres Utility Functions
//===--------------------------------------------------------------------===//

//===--------------------------------------------------------------------===//
// Getters
//===--------------------------------------------------------------------===//

/**
 * @brief Get the pg class tuple
 * @param tuple relevant tuple if it exists, NULL otherwise
 */
HeapTuple
GetPGClassTuple(Oid relation_id){
  Relation pg_class_rel;
  HeapTuple tuple = NULL;

  StartTransactionCommand();

  // Open pg_class table
  pg_class_rel = heap_open(RelationRelationId, AccessShareLock);

  // Search the pg_class table with given relation id
  tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relation_id));
  if (!HeapTupleIsValid(tuple)) {
    elog(DEBUG2, "cache lookup failed for relation %u", relation_id);
    return NULL;
  }

  heap_close(pg_class_rel, AccessShareLock);
  CommitTransactionCommand();

  return tuple;
}

/**
 * @brief Get the pg class tuple
 * @param tuple relevant tuple if it exists, NULL otherwise
 */
HeapTuple
GetPGClassTuple(const char *relation_name){
  Relation pg_class_rel;
  HeapTuple tuple = NULL;
  HeapScanDesc scan;

  StartTransactionCommand();

  // Open pg_class table
  pg_class_rel = heap_open(RelationRelationId, AccessShareLock);

  // Search the pg_class table with given relation name
  scan = heap_beginscan_catalog(pg_class_rel, 0, NULL);

  while (HeapTupleIsValid(tuple = heap_getnext(scan, ForwardScanDirection))) {
    Form_pg_class pgclass = (Form_pg_class) GETSTRUCT(tuple);

    if( pgclass->relnamespace==PG_PUBLIC_NAMESPACE){
      if(strcmp(NameStr(pgclass->relname), relation_name) == 0)
        return tuple;
    }
  }

  heap_endscan(scan);
  heap_close(pg_class_rel, AccessShareLock);

  CommitTransactionCommand();

  return NULL;
}

//===--------------------------------------------------------------------===//
// Oid <--> Name
//===--------------------------------------------------------------------===//

/**
 * @brief Getting the relation name
 * @param relation_id relation id
 * @return Tuple if valid relation_id, otherwise null
 */
HeapTuple
GetRelationName(Oid relation_id){
  HeapTuple tuple;
  Form_pg_class pg_class;
  char* relation_name;

  tuple = GetPGClassTuple(relation_id);
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
Oid
GetRelationOid(const char *relation_name){
  Oid relation_oid = InvalidOid;
  HeapTuple tuple;
  Form_pg_class pg_class;
  char* rel_name;

  tuple = GetPGClassTuple(relation_name);
  if (!HeapTupleIsValid(tuple)) {
    return InvalidOid;
  }

  // Get relation oid
  pg_class = (Form_pg_class) GETSTRUCT(tuple);
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
int 
GetNumberOfAttributes(Oid relation_id) {
  HeapTuple tuple;
  Form_pg_class pg_class;
  int num_atts = -1;

  tuple = GetPGClassTuple(relation_id);
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
float 
GetNumberOfTuples(Oid relation_id){
  HeapTuple tuple;
  Form_pg_class pg_class;
  float num_tuples;

  tuple = GetPGClassTuple(relation_id);
  if (!HeapTupleIsValid(tuple)) {
    return num_tuples;
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
Oid 
GetCurrentDatabaseOid(void){
  return MyDatabaseId;
}


/**
 * @Determine whether table exists in the *current* database or not
 * @param table_name table name
 * @return true or false depending on whether table exists or not.
 */
bool RelationExists(const char* relation_name) {
  HeapTuple tuple;
  Form_pg_class pg_class;

  tuple = GetPGClassTuple(relation_name);
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
void GetTableList(bool catalog_only) {
  Relation pg_class_rel;
  HeapScanDesc scan;
  HeapTuple tuple;

  StartTransactionCommand();

  // Scan pg class table
  pg_class_rel = heap_open(RelationRelationId, AccessShareLock);
  scan = heap_beginscan_catalog(pg_class_rel, 0, NULL);

  while (HeapTupleIsValid(tuple = heap_getnext(scan, ForwardScanDirection))) {
    Form_pg_class pg_class = (Form_pg_class) GETSTRUCT(tuple);

    // Check if we only need catalog tables or not ?
    if(catalog_only == false) {
      elog(LOG, "pgclass->relname :: %s  \n", NameStr(pg_class->relname ) );
    }
    else if(pg_class->relnamespace==PG_PUBLIC_NAMESPACE) {
      elog(LOG, "pgclass->relname :: %s  \n", NameStr(pg_class->relname ) );
    }

  }

  heap_endscan(scan);
  heap_close(pg_class_rel, AccessShareLock);

  CommitTransactionCommand();
}

/**
 * @brief Print all databases using catalog table pg_database
 */
void GetDatabaseList(void) {
  Relation pg_database_rel;
  HeapScanDesc scan;
  HeapTuple tup;

  StartTransactionCommand();

  // Scan pg database table
  pg_database_rel = heap_open(DatabaseRelationId, AccessShareLock);
  scan = heap_beginscan_catalog(pg_database_rel, 0, NULL);

  while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))  {
    Form_pg_database pg_database = (Form_pg_database) GETSTRUCT(tup);
    elog(LOG, "pgdatabase->datname  :: %s\n", NameStr(pg_database->datname) );
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
 * @param num_of_tuples number of tuples
 */
void
SetNumberOfTuples(Oid relation_id, float num_tuples) {
  Relation pg_class_rel;
  HeapTuple tuple;
  Form_pg_class pgclass;
  bool dirty;

  StartTransactionCommand();

  pg_class_rel = heap_open(RelationRelationId,RowExclusiveLock);

  tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relation_id));
  if (!HeapTupleIsValid(tuple))
    elog(ERROR, "cache lookup failed for relation %u", relation_id);

  pgclass = (Form_pg_class) GETSTRUCT(tuple);

  dirty = false;
  if (pgclass->reltuples != (float4) num_tuples)
  {
    pgclass->reltuples = (float4) num_tuples;
    dirty = true;
  }

  /* If anything changed, write out the tuple. */
  if (dirty) {
    simple_heap_update(pg_class_rel, &tuple->t_data->t_ctid, tuple);
  }

  heap_close(pg_class_rel, RowExclusiveLock);

  CommitTransactionCommand();
}

/**
 * @brief Setting the user table stats
 * @param relation_id relation id
 */
struct user_pg_database {
  char datname[10];
  int datdba;
  int encoding;
};

typedef struct user_pg_database *Form_user_pg_database;
void  SetUserTableStats(Oid relation_id)
{
  Relation rel;
  HeapTuple newtup;
  //HeapTuple tup, newtup;
  Oid relid;
  Form_user_pg_database userpgdatabase;

  StartTransactionCommand();
  rel = heap_open(relation_id, RowExclusiveLock);
  relid = RelationGetRelid(rel);

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

  /* update tuple */
  simple_heap_update(rel, &newtup->t_self, newtup);

  printf("%s %d\n", __func__, __LINE__);
  heap_freetuple(newtup); // It may incur segmentation fault

  /*
   * Close relation, but keep lock till commit.
   */
  heap_close(rel, RowExclusiveLock);
  CommitTransactionCommand();
}


/**
 * Initialize Peloton
 * This function constructs tables in all databases
 */
bool InitPeloton(void)
{
  // Relations for catalog tables
  Relation pg_class_rel;
  Relation pg_attribute_rel;

  HeapScanDesc scan_pg_class;
  HeapTuple tuple_for_pg_class;

  int column_itr;
  bool ret;

  elog(LOG, "Initializing Peloton");

  StartTransactionCommand();

  pg_class_rel = heap_open(RelationRelationId, AccessShareLock);
  pg_attribute_rel = heap_open(AttributeRelationId, AccessShareLock);

  scan_pg_class = heap_beginscan_catalog(pg_class_rel, 0, NULL);

  //TODO :: Make this one single loop
  while (HeapTupleIsValid(tuple_for_pg_class = heap_getnext(scan_pg_class, ForwardScanDirection))) {
    Form_pg_class pgclass = (Form_pg_class) GETSTRUCT(tuple_for_pg_class);

    // Create only user tables
    if( pgclass->relnamespace==PG_PUBLIC_NAMESPACE && ( pgclass->relkind == 'r' ||  pgclass->relkind == 'i'))
    {
      // create columninfo as much as attnum
      int attnum =  pgclass->relnatts ;
      if( attnum > 0 )
      {
        HeapScanDesc scan_pg_attribute;
        HeapTuple tuple_for_pg_attribute;
        ColumnInfo ddl_columnInfo[ pgclass->relnatts ] ;
        Oid table_oid = HeapTupleHeaderGetOid(tuple_for_pg_class->t_data);

        scan_pg_attribute = heap_beginscan_catalog(pg_attribute_rel, 0, NULL);
        column_itr = 0;
        while (HeapTupleIsValid(tuple_for_pg_attribute = heap_getnext(scan_pg_attribute, ForwardScanDirection))) {
          Form_pg_attribute pgattribute = (Form_pg_attribute) GETSTRUCT(tuple_for_pg_attribute);
          if( pgattribute->attrelid == table_oid )
          {
            // Skip system columns..
            if( strcmp( NameStr(pgattribute->attname),"cmax" ) &&
                strcmp( NameStr(pgattribute->attname),"cmin" ) &&
                strcmp( NameStr(pgattribute->attname),"ctid" ) &&
                strcmp( NameStr(pgattribute->attname),"xmax" ) &&
                strcmp( NameStr(pgattribute->attname),"xmin" ) &&
                strcmp( NameStr(pgattribute->attname),"tableoid" ) )
            {
              ddl_columnInfo[column_itr].type = pgattribute->atttypid;
              ddl_columnInfo[column_itr].column_offset = column_itr;
              ddl_columnInfo[column_itr].column_length = pgattribute->attlen;
              strcpy(ddl_columnInfo[column_itr].name, NameStr(pgattribute->attname));
              ddl_columnInfo[column_itr].allow_null = ! pgattribute->attnotnull;
              ddl_columnInfo[column_itr].is_inlined = false; // true for int, double, char, timestamp..
              column_itr++;
            } // end if
          } // end if
        } // end while

        heap_endscan(scan_pg_attribute);

        // Create the table
        if( pgclass->relkind == 'r' )
        {
          ret = DDLCreateTable( NameStr(pgclass->relname) , ddl_columnInfo, column_itr);
          if( ret )  printf("Create Table \"%s\" in Peloton\n", NameStr(pgclass->relname));
          else       fprintf(stderr, "DDLCreateTable :: %d \n", ret);
        }
        // Create the index
        else if( pgclass->relkind == 'i')
        {
          Relation pg_index_rel;
          HeapScanDesc scan_pg_index;
          HeapTuple tuple_for_pg_index;
          pg_index_rel = heap_open(IndexRelationId, AccessShareLock);
          scan_pg_index = heap_beginscan_catalog(pg_index_rel, 0, NULL);

          while (HeapTupleIsValid(tuple_for_pg_index = heap_getnext(scan_pg_index, ForwardScanDirection))) {
            Form_pg_index pgindex = (Form_pg_index) GETSTRUCT(tuple_for_pg_index);

            if( pgindex->indexrelid == table_oid )
            {
              DDLCreateIndex(NameStr(pgclass->relname), get_rel_name(pgindex->indrelid), 0, pgindex->indisunique, ddl_columnInfo, column_itr);
              if( ret )  printf("Create Index \"%s\" in Peloton\n", NameStr(pgclass->relname));
              else       fprintf(stderr, "DDLCreateIndex :: %d \n", ret);
              break;
            }
          }

          heap_endscan(scan_pg_index);
          heap_close(pg_index_rel, AccessShareLock);
        }
      }else
      {
        // Create Table without column info
        ret = DDLCreateTable( NameStr(pgclass->relname) , NULL, 0);
        if( ret )  printf("Create Table \"%s\" in Peloton\n", NameStr(pgclass->relname));
        else       fprintf(stderr, "DDLCreateTable :: %d \n", ret);
      }
    } // end if

  } // end while

  heap_endscan(scan_pg_class);
  heap_close(pg_attribute_rel, AccessShareLock);
  heap_close(pg_class_rel, AccessShareLock);

  CommitTransactionCommand();

  elog(LOG, "Finished initializing Peloton");

  return true;
}



