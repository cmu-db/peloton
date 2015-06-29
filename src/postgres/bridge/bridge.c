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

/**
 * @brief Getting the relation name
 * @param relation_id relation id
 */
char* 
GetRelationName(Oid relation_id){
  Relation pg_class_rel;
  HeapTuple tuple;
  Form_pg_class pgclass;

  StartTransactionCommand();

  //open pg_class table
  pg_class_rel = heap_open(RelationRelationId,AccessShareLock);

  //search the table with given relation id from pg_class table
  tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relation_id));
  if (!HeapTupleIsValid(tuple))
  {
    //Check whether relation id is valid or not
    elog(ERROR, "cache lookup failed for relation %u", relation_id);
  }

  pgclass = (Form_pg_class) GETSTRUCT(tuple);

  heap_close(pg_class_rel, AccessShareLock);

  CommitTransactionCommand();

  return NameStr(pgclass->relname);
}

/**
 * @brief Getting the number of attributes.
 * @param relation_id relation id
 */
int 
GetNumberOfAttributes(Oid relation_id) {
  Relation pg_class_rel;
  HeapTuple tuple;
  Form_pg_class pgclass;

  StartTransactionCommand();

  pg_class_rel = heap_open(RelationRelationId, AccessShareLock);

  tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relation_id));
  if (!HeapTupleIsValid(tuple))
    elog(ERROR, "cache lookup failed for relation %u", relation_id);

  pgclass = (Form_pg_class) GETSTRUCT(tuple);

  heap_close(pg_class_rel, AccessShareLock);

  CommitTransactionCommand();

  return pgclass->relnatts;
}

/**
 * @brief Getting the number of tuples.
 * @param relation_id relation id
 */
float 
GetNumberOfTuples(Oid relation_id){
  Relation pg_class_rel;
  HeapTuple tuple;
  Form_pg_class pgclass;

  StartTransactionCommand();

  pg_class_rel = heap_open(RelationRelationId,AccessShareLock);

  tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relation_id));
  if (!HeapTupleIsValid(tuple))
    elog(ERROR, "cache lookup failed for relation %u", relation_id);

  pgclass = (Form_pg_class) GETSTRUCT(tuple);

  heap_close(pg_class_rel, AccessShareLock);

  CommitTransactionCommand();

  return pgclass->reltuples;
}

/**
 * @brief Getting the current database Oid
 */
Oid 
GetCurrentDatabaseOid(void){
  return MyDatabaseId;
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
 * @brief Printing all databases from catalog table, i.e., pg_database
 */
void GetDatabaseList(void) {
  Relation rel;
  HeapScanDesc scan;
  HeapTuple tup;

  StartTransactionCommand();

  rel = heap_open(DatabaseRelationId, AccessShareLock);
  scan = heap_beginscan_catalog(rel, 0, NULL);

  while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))  {
    Form_pg_database pgdatabase = (Form_pg_database) GETSTRUCT(tup);
    printf(" pgdatabase->datname  :: %s\n", NameStr(pgdatabase->datname) );
  }

  heap_endscan(scan);
  heap_close(rel, AccessShareLock);

  CommitTransactionCommand();
}

/**
 * @brief Printing all tables of current database from catalog table, i.e., pg_class
 */
void GetTableList(void) {
  Relation pg_class_rel;
  HeapScanDesc scan;
  HeapTuple tuple;

  StartTransactionCommand();

  pg_class_rel = heap_open(RelationRelationId, AccessShareLock);
  scan = heap_beginscan_catalog(pg_class_rel, 0, NULL);

  while (HeapTupleIsValid(tuple = heap_getnext(scan, ForwardScanDirection))) {
    Form_pg_class pgclass = (Form_pg_class) GETSTRUCT(tuple);
    printf(" pgclass->relname    :: %s  \n", NameStr(pgclass->relname ) );
  }

  heap_endscan(scan);
  heap_close(pg_class_rel, AccessShareLock);

  CommitTransactionCommand();

}

/**
 * @brief Printing all public tables of current database from catalog table, i.e., pg_class
 */
void GetPublicTableList(void) {
  Relation rel;
  HeapScanDesc scan;
  HeapTuple tuple;

  StartTransactionCommand();

  rel = heap_open(RelationRelationId, AccessShareLock);
  scan = heap_beginscan_catalog(rel, 0, NULL);

  // TODO: Whar are we trying to do here ?
  while (HeapTupleIsValid(tuple = heap_getnext(scan, ForwardScanDirection))) {
    Form_pg_class pgclass = (Form_pg_class) GETSTRUCT(tuple);
    // Print out only public tables
    if( pgclass->relnamespace==PG_PUBLIC_NAMESPACE)
      printf(" pgclass->relname    :: %s  \n", NameStr(pgclass->relname ) );
  }

  heap_endscan(scan);
  heap_close(rel, AccessShareLock);

  CommitTransactionCommand();

}

/**
 * @Determin whether 'table_name' table exists in the current database or not
 * @param table_name table name
 */
bool IsThisTableExist(const char* table_name) {
  Relation rel;
  HeapScanDesc scan;
  HeapTuple tuple;

  StartTransactionCommand();

  rel = heap_open(RelationRelationId, AccessShareLock);
  scan = heap_beginscan_catalog(rel, 0, NULL);

  while (HeapTupleIsValid(tuple = heap_getnext(scan, ForwardScanDirection))) {
    Form_pg_class pgclass = (Form_pg_class) GETSTRUCT(tuple);
    const char* current_table_name = NameStr(pgclass->relname);

    //Compare current table name and given table name
    if( pgclass->relnamespace==PG_PUBLIC_NAMESPACE &&
        strcmp( current_table_name, table_name ) == 0)
      return true;
  }

  heap_endscan(scan);
  heap_close(rel, AccessShareLock);

  CommitTransactionCommand();

  return false;
}

/**
 * Initialize Peloton
 * This function constructs tables in current database
 * @param dbname current database
 */
bool InitPeloton(const char* dbname)
{
  // Relations for catalog tables
  Relation pg_class_rel;   
  Relation pg_attribute_rel;

  HeapScanDesc scan_pg_class;
  HeapTuple tuple_for_pg_class;

  int column_itr;
  bool ret;

  printf("Process Id : %d \n", getpid());
  printf("################################################\n");
  printf("#### Initialize Peloton Database \"%s\" #### \n", dbname);
  printf("################################################\n");

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
        DDL_ColumnInfo ddl_columnInfo[ pgclass->relnatts ] ;
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
          ret = DDL_CreateTable( NameStr(pgclass->relname) , ddl_columnInfo, column_itr);
          if( ret )  printf("Create Table \"%s\" in Peloton\n", NameStr(pgclass->relname));
          else       fprintf(stderr, "DDL_CreateTable :: %d \n", ret);
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
              DDL_CreateIndex(NameStr(pgclass->relname), get_rel_name(pgindex->indrelid), 0, pgindex->indisunique, ddl_columnInfo, column_itr);
              if( ret )  printf("Create Index \"%s\" in Peloton\n", NameStr(pgclass->relname));
              else       fprintf(stderr, "DDL_CreateIndex :: %d \n", ret);
              break;
            }
          }

          heap_endscan(scan_pg_index);
          heap_close(pg_index_rel, AccessShareLock);
        }
      }else
      {
        // Create Table without column info
        ret = DDL_CreateTable( NameStr(pgclass->relname) , NULL, 0);
        if( ret )  printf("Create Table \"%s\" in Peloton\n", NameStr(pgclass->relname));
        else       fprintf(stderr, "DDL_CreateTable :: %d \n", ret);
      }
    } // end if

  } // end while

  heap_endscan(scan_pg_class);
  heap_close(pg_attribute_rel, AccessShareLock);
  heap_close(pg_class_rel, AccessShareLock);

  CommitTransactionCommand();

  return true;
}

/*
 * given a table name, look up the OID
 * @param table_name table name
 */
unsigned int
GetRelationOidFromRelationName(const char *table_name){
  unsigned int relation_oid = 0;
  Relation pg_class_rel;

  HeapScanDesc scan;

  HeapTuple tuple;

  pg_class_rel = heap_open(RelationRelationId, AccessShareLock);

  scan = heap_beginscan_catalog(pg_class_rel, 0, NULL);

  while (HeapTupleIsValid(tuple = heap_getnext(scan, ForwardScanDirection))) {
    Form_pg_class pgclass = (Form_pg_class) GETSTRUCT(tuple);
    if( pgclass->relnamespace==PG_PUBLIC_NAMESPACE){
      if( strcmp( NameStr(pgclass->relname), table_name) == 0 )
        relation_oid = HeapTupleHeaderGetOid(tuple->t_data);
    }
  } // end while

  heap_endscan(scan);
  heap_close(pg_class_rel, AccessShareLock);

  return relation_oid;
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

void FunctionTest(void)
{
  int n;
  n = GetNumberOfAttributes(16388);
  printf("n %d\n",n);
  n = GetNumberOfAttributes(16385);
  printf("n %d\n",n);
  n = GetNumberOfAttributes(DatabaseRelationId);
  printf("n %d\n",n);
  n = GetNumberOfAttributes(RelationRelationId);
  printf("n %d\n",n);
}
