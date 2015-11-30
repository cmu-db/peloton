#include "backend/bridge/ddl/raw_database_info.h"
#include "backend/bridge/ddl/ddl_database.h"
#include "backend/bridge/ddl/format_transformer.h"
#include "backend/common/exception.h"

#include "catalog/pg_class.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_attribute.h"
#include "access/xact.h"
#include "catalog/pg_database.h"
#include "commands/dbcommands.h"
#include "common/fe_memutils.h"
#include "utils/ruleutils.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/rel.h"

//#include <sys/types.h>
//#include <unistd.h>
//#include <cassert>

//TODO :: Scrubbing header files

//#include "backend/bridge/ddl/ddl_table.h"
//#include "backend/storage/database.h"

//#include "utils/syscache.h"

namespace peloton {
namespace bridge {

void raw_database_info::CollectRawTableAndIndex(void){

  // Open the pg_class and pg_attribute catalog tables
  Relation pg_class_rel = heap_open(RelationRelationId, AccessShareLock);
  Relation pg_attribute_rel = heap_open(AttributeRelationId, AccessShareLock);

  HeapScanDesc pg_class_scan = heap_beginscan_catalog(pg_class_rel, 0, NULL);

  // Go over all tuples in "pg_class"
  // pg_class has info about tables and everything else that has columns or is
  // otherwise similar to a table.
  // This includes indexes, sequences, views, composite types, and some kinds of
  // special relation.
  // So, each tuple can correspond to a table, index, etc.
  while (1) {
    // Get next tuple from pg_class
    HeapTuple pg_class_tuple = heap_getnext(pg_class_scan, ForwardScanDirection);

    if (!HeapTupleIsValid(pg_class_tuple)) break;

    Form_pg_class pg_class = (Form_pg_class)GETSTRUCT(pg_class_tuple);
    std::string relation_name;
    if( NameStr(pg_class->relname) != NULL){
      relation_name = NameStr(pg_class->relname);
    }
    char relation_kind = pg_class->relkind;

    // Handle only user-defined structures, not pg-catalog structures
    if (pg_class->relnamespace != PG_PUBLIC_NAMESPACE) continue;

    // TODO: Currently, we only handle relations and indexes
    if (pg_class->relkind != 'r' && pg_class->relkind != 'i') {
      continue;
    }

    // We only support tables with atleast one attribute
    int attnum = pg_class->relnatts;
    if(attnum == 0) {
      throw CatalogException("We only support tables with atleast one attribute");
    }

    // Get the tuple oid
    // This can be a relation oid or index oid etc.
    oid_t relation_oid = HeapTupleHeaderGetOid(pg_class_tuple->t_data);
    std::vector<raw_column_info> raw_columns = CollectRawColumn(relation_oid, pg_attribute_rel);

    switch (relation_kind) {

      case 'r':  {
        AddRawTable(relation_oid, relation_name, raw_columns);
        break;
      }

      case 'i':  {
        AddRawIndex(relation_oid, relation_name, raw_columns);
        break;
      }

      default:  {
        break;
      }
    }
    raw_columns.clear();
  }
  heap_endscan(pg_class_scan);
  heap_close(pg_attribute_rel, AccessShareLock);
  heap_close(pg_class_rel, AccessShareLock);
}

void raw_database_info::CollectRawForeignKeys(void) {

  Relation pg_constraint_rel;
  HeapScanDesc pg_constraint_scan;
  HeapTuple pg_constraint_tuple;

  pg_constraint_rel = heap_open(ConstraintRelationId, AccessShareLock);
  pg_constraint_scan = heap_beginscan_catalog(pg_constraint_rel, 0, NULL);

  // Go over the pg_constraint catalog table looking for foreign key constraints
  while (1) {
    Form_pg_constraint pg_constraint;

    pg_constraint_tuple = heap_getnext(pg_constraint_scan, ForwardScanDirection);
    if (!HeapTupleIsValid(pg_constraint_tuple)) break;

    pg_constraint = (Form_pg_constraint)GETSTRUCT(pg_constraint_tuple);

    // We only handle foreign key constraints here
    if (pg_constraint->contype != 'f') continue;

    // store raw information from here..


    // TODO :: Find better way..
    // column offsets, count
    bool isNull;
    Datum curr_datum = heap_getattr(pg_constraint_tuple, Anum_pg_constraint_conkey,
                                    RelationGetDescr(pg_constraint_rel), &isNull);
    Datum ref_datum = heap_getattr(pg_constraint_tuple, Anum_pg_constraint_confkey,
                                    RelationGetDescr(pg_constraint_rel), &isNull);

    ArrayType *curr_arr = DatumGetArrayTypeP(curr_datum);
    ArrayType *ref_arr = DatumGetArrayTypeP(ref_datum);
    int16 *curr_attnums = (int16 *)ARR_DATA_PTR(curr_arr);
    int16 *ref_attnums = (int16 *)ARR_DATA_PTR(ref_arr);
    int source_numkeys = ARR_DIMS(curr_arr)[0];
    int sink_numkeys = ARR_DIMS(ref_arr)[0];

    std::vector<int> source_column_offsets;
    std::vector<int> sink_column_offsets;

    // Populate foreign key column names
    for (int source_key_itr = 0; source_key_itr < source_numkeys;
         source_key_itr++) {
      AttrNumber attnum = curr_attnums[source_key_itr];
      source_column_offsets.push_back(attnum);
    }

    // Populate primary key column names
    for (int sink_key_itr = 0; sink_key_itr < sink_numkeys; sink_key_itr++) {
      AttrNumber attnum = ref_attnums[sink_key_itr];
      sink_column_offsets.push_back(attnum);
    }
    std::string fk_name;
    if( NameStr(pg_constraint->conname) != NULL){
      fk_name = std::string(NameStr(pg_constraint->conname));
    }

    raw_foreign_key_info raw_foreignkey(pg_constraint->conrelid,
                                        pg_constraint->confrelid,
                                        source_column_offsets,
                                        sink_column_offsets,
                                        pg_constraint->confupdtype,
                                        pg_constraint->confdeltype,
                                        fk_name);
    AddRawForeignKey(raw_foreignkey);
  }

  heap_endscan(pg_constraint_scan);
  heap_close(pg_constraint_rel, AccessShareLock);
}

/**
 * @brief collect raw column
 * @param relation oid
 * @param pg_attribute_rel pg_attribute catalog relation
 * @return raw columns
 */
std::vector<raw_column_info> raw_database_info::CollectRawColumn(oid_t relation_oid, Relation pg_attribute_rel) {
  HeapScanDesc pg_attribute_scan;
  HeapTuple pg_attribute_tuple;

  std::vector<raw_column_info> raw_columns;

  // Scan the pg_attribute table for the relation oid we are interested in.
  pg_attribute_scan = heap_beginscan_catalog(pg_attribute_rel, 0, NULL);

  // Go over all attributes in "pg_attribute" looking for any entries
  // matching the given tuple oid.
  // For instance, this means the columns associated with a given relation oid.
  while (1) {
    Form_pg_attribute pg_attribute;

    // Get next <relation, attribute> tuple from pg_attribute table
    pg_attribute_tuple = heap_getnext(pg_attribute_scan, ForwardScanDirection);

    if (!HeapTupleIsValid(pg_attribute_tuple)) break;

    // Check the relation oid
    pg_attribute = (Form_pg_attribute)GETSTRUCT(pg_attribute_tuple);

    if (pg_attribute->attrelid == relation_oid) {
      // Skip system columns in the attribute list
      if (strcmp(NameStr(pg_attribute->attname), "cmax") &&
          strcmp(NameStr(pg_attribute->attname), "cmin") &&
          strcmp(NameStr(pg_attribute->attname), "ctid") &&
          strcmp(NameStr(pg_attribute->attname), "xmax") &&
          strcmp(NameStr(pg_attribute->attname), "xmin") &&
          strcmp(NameStr(pg_attribute->attname), "tableoid")) {
        std::vector<raw_constraint_info> raw_constraints;

        PostgresValueFormat postgresValueFormat( pg_attribute->atttypid, pg_attribute->atttypmod, pg_attribute->attlen); 
        PelotonValueFormat pelotonValueFormat = FormatTransformer::TransformValueFormat(postgresValueFormat);

        ValueType value_type = pelotonValueFormat.GetType();
        int column_length = pelotonValueFormat.GetLength();
        bool is_inlined = pelotonValueFormat.IsInlined();

        std::string const_name;

        // NOT NULL constraint
        if (pg_attribute->attnotnull) {
          raw_constraint_info raw_constraint(CONSTRAINT_TYPE_NOTNULL, const_name);
          raw_constraints.push_back(raw_constraint);
        }

        // DEFAULT value constraint
        if (pg_attribute->atthasdef) {
          // Setting default constraint expression
          Relation relation = heap_open(relation_oid, AccessShareLock);

          raw_constraint_info raw_constraint(CONSTRAINT_TYPE_DEFAULT, const_name);

          int num_defva = relation->rd_att->constr->num_defval;
          for (int def_itr = 0; def_itr < num_defva; def_itr++) {
            if (pg_attribute->attnum ==
                relation->rd_att->constr->defval[def_itr].adnum) {
              char *default_expression =
                  relation->rd_att->constr->defval[def_itr].adbin;
              raw_constraint.SetDefaultExpr(static_cast<Node *>(stringToNode(default_expression)));
            }
          }
          raw_constraints.push_back(raw_constraint);

          heap_close(relation, AccessShareLock);
        }

        std::string colname;
        if(NameStr(pg_attribute->attname) != NULL){
          colname = std::string(NameStr(pg_attribute->attname)); 
        }

        raw_column_info raw_column(value_type, 
                                   column_length, 
                                   colname,
                                   is_inlined,
                                   raw_constraints);
        raw_constraints.clear();
        raw_columns.push_back(raw_column);
      }
    }
  }

  heap_endscan(pg_attribute_scan);

  return raw_columns;
}

void raw_database_info::AddRawTable(oid_t table_oid,
                                    std::string table_name,
                                    std::vector<raw_column_info> raw_columns){

  raw_table_info raw_table(table_oid, table_name, raw_columns);
  raw_tables.push_back(raw_table);
}

void raw_database_info::AddRawIndex(oid_t index_oid,
                                    std::string index_name,
                                    std::vector<raw_column_info> raw_columns){

  Relation pg_index_rel;
  HeapScanDesc pg_index_scan;
  HeapTuple pg_index_tuple;

  pg_index_rel = heap_open(IndexRelationId, AccessShareLock);
  pg_index_scan = heap_beginscan_catalog(pg_index_rel, 0, NULL);

  // Go over the pg_index catalog table looking for indexes
  // that are associated with this table
  while (1) {
    Form_pg_index pg_index;

    pg_index_tuple = heap_getnext(pg_index_scan, ForwardScanDirection);
    if (!HeapTupleIsValid(pg_index_tuple)) break;

    pg_index = (Form_pg_index)GETSTRUCT(pg_index_tuple);

    // Search for the tuple in pg_index corresponding to our index
    if (pg_index->indexrelid == index_oid) {
      std::vector<std::string> key_column_names;

      for (auto raw_column : raw_columns) {
        key_column_names.push_back(raw_column.GetColName());
      }

      IndexType method_type = INDEX_TYPE_BTREE;
      IndexConstraintType type;

      if (pg_index->indisprimary) {
        type = INDEX_CONSTRAINT_TYPE_PRIMARY_KEY;
      } else if (pg_index->indisunique) {
        type = INDEX_CONSTRAINT_TYPE_UNIQUE;
      } else {
        type = INDEX_CONSTRAINT_TYPE_DEFAULT;
      }

      // Store all index information here
      // This is required because we can only create indexes at once
      // after all tables are created
      // The order of table and index entries in pg_class table can be arbitrary

      std::string table_name;
      if( get_rel_name(pg_index->indrelid) != NULL){
        table_name = std::string(get_rel_name(pg_index->indrelid));
      }

      raw_index_info raw_index(index_oid, 
                               index_name,
                               table_name,
                               method_type,
                               type,
                               pg_index->indisunique,
                               key_column_names);

      raw_indexes.push_back(raw_index);

      break;
    }
  }

  heap_endscan(pg_index_scan);
  heap_close(pg_index_rel, AccessShareLock);
}

void raw_database_info::AddRawForeignKey(raw_foreign_key_info raw_foreign_key){
  raw_foreign_keys.push_back(raw_foreign_key);
}

bool raw_database_info::CreateDatabase(void) const{
  bool status = DDLDatabase::CreateDatabase(GetDbOid());
  return status;
}

void raw_database_info::CreateTables(void) const{
  for(auto raw_table : raw_tables){
    raw_table.CreateTable();
  }
}

void raw_database_info::CreateIndexes(void) const{
  for(auto raw_index : raw_indexes){
    raw_index.CreateIndex();
  }
}

void raw_database_info::CreateForeignkeys(void) const{
  for(auto raw_foreign_key : raw_foreign_keys){
    raw_foreign_key.CreateForeignkey();
  }
}

}  // namespace bridge
}  // namespace peloton
