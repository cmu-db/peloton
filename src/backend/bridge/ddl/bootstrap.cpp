//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// bootstrap.cpp
//
// Identification: src/backend/bridge/ddl/bootstrap.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sys/types.h>
#include <unistd.h>
#include <cassert>

#include "backend/bridge/ddl/bridge.h"
#include "backend/bridge/ddl/bootstrap.h"
#include "backend/bridge/ddl/bootstrap_utils.h"
#include "backend/bridge/ddl/ddl_database.h"
#include "backend/bridge/ddl/ddl_table.h"
#include "backend/bridge/ddl/format_transformer.h"
#include "backend/storage/database.h"
#include "backend/common/logger.h"


#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_class.h"
#include "catalog/pg_database.h"
#include "catalog/pg_namespace.h"
#include "commands/dbcommands.h"
#include "common/fe_memutils.h"
#include "utils/ruleutils.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

namespace peloton {
namespace bridge {

/**
 * @brief Collecting information regarding tables, indexes, foreign key from
 * Postgres for bootstrap
 * @return raw structure
 */
raw_database_info *Bootstrap::GetRawDatabase(void) {
  // Create and initialize raw database
  raw_database_info *raw_database = Bootstrap::InitRawDatabase();

  std::vector<raw_table_info *> raw_tables;
  std::vector<raw_index_info *> raw_indexes;
  std::vector<raw_foreign_key_info *> raw_foreignkeys;

  // Get objects from Postgres
  GetRawTableAndIndex(raw_tables, raw_indexes);
  GetRawForeignKeys(raw_foreignkeys);

  // Copy collected objects to raw_database (simple pointer copy)
  BootstrapUtils::CopyRawTables(raw_database, raw_tables);
  BootstrapUtils::CopyRawIndexes(raw_database, raw_indexes);
  BootstrapUtils::CopyRawForeignkeys(raw_database, raw_foreignkeys);

  return raw_database;
}

/**
 * @brief This function constructs all the user-defined tables and indices in
 * all databases
 * @params raw_database raw data that contains information about tables,
 * indexes, foreign key to create in Peloton
 * @return true or false, depending on whether we could bootstrap.
 */
bool Bootstrap::BootstrapPeloton(raw_database_info *raw_database) {
  // create the database with current database id
  elog(LOG, "Initializing database %s(%u) in Peloton",
       raw_database->database_name, raw_database->database_oid);
  bool status = DDLDatabase::CreateDatabase(raw_database->database_oid);

  // skip if we already initialize current database
  if (status == false) return false;

  // Create objects in Peloton
  CreateTables(raw_database->raw_tables, raw_database->table_count);
  CreateIndexes(raw_database->raw_indexes, raw_database->index_count);
  CreateForeignkeys(raw_database->raw_foreignkeys,
                    raw_database->foreignkey_count);

  auto &manager = catalog::Manager::GetInstance();

  // TODO: Update stats
  //auto db = manager.GetDatabaseWithOid(Bridge::GetCurrentDatabaseOid());
  //db->UpdateStats(peloton_status, false);

  // Verbose mode
  //std::cout << "Print db :: \n"<<*db << std::endl;

  elog(LOG, "Finished initializing Peloton");
  return true;
}

/**
 * @brief initialize raw database
 * @return raw database
 */
raw_database_info *Bootstrap::InitRawDatabase() {
  // Set databse oid and name
  raw_database_info *raw_database =
      (raw_database_info *)palloc(sizeof(raw_database_info));
  raw_database->database_oid = MyDatabaseId;
  raw_database->database_name =
      BootstrapUtils::CopyString(get_database_name(MyDatabaseId));
  return raw_database;
}

/**
 * @brief construct raw tables and indexes
 * @param raw tables
 * @param raw indexes
 */
void Bootstrap::GetRawTableAndIndex(
    std::vector<raw_table_info *> &raw_tables,
    std::vector<raw_index_info *> &raw_indexes) {
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
    HeapTuple pg_class_tuple =
        heap_getnext(pg_class_scan, ForwardScanDirection);

    if (!HeapTupleIsValid(pg_class_tuple)) break;

    Form_pg_class pg_class = (Form_pg_class)GETSTRUCT(pg_class_tuple);
    std::string relation_name = NameStr(pg_class->relname);
    char relation_kind = pg_class->relkind;

    // Handle only user-defined structures, not pg-catalog structures
    if (pg_class->relnamespace != PG_PUBLIC_NAMESPACE) continue;

    // TODO: Currently, we only handle relations and indexes
    if (pg_class->relkind != 'r' && pg_class->relkind != 'i') {
      continue;
    }

    // We only support tables with atleast one attribute
    int attnum = pg_class->relnatts;
    assert(attnum > 0);

    // Get the tuple oid
    // This can be a relation oid or index oid etc.
    Oid relation_oid = HeapTupleHeaderGetOid(pg_class_tuple->t_data);
    std::vector<raw_column_info *> raw_columns =
        GetRawColumn(relation_oid, relation_kind, pg_attribute_rel);

    switch (relation_kind) {
      case 'r': {
        raw_table_info *raw_table =
            GetRawTable(relation_oid, relation_name, raw_columns);
        raw_tables.push_back(raw_table);
      } break;
      case 'i': {
        raw_index_info *raw_index =
            GetRawIndex(relation_oid, relation_name, raw_columns);
        raw_indexes.push_back(raw_index);
      } break;
    }
    raw_columns.clear();
  }
  heap_endscan(pg_class_scan);
  heap_close(pg_attribute_rel, AccessShareLock);
  heap_close(pg_class_rel, AccessShareLock);
}

/**
 * @brief construct raw table with table oid, table name, raw columns
 * @param table oid
 * @param table name
 * @param raw columns
 * @return raw table
 */
raw_table_info *Bootstrap::GetRawTable(
    oid_t table_oid, std::string table_name,
    std::vector<raw_column_info *> raw_columns) {
  raw_table_info *raw_table = (raw_table_info *)palloc(sizeof(raw_table_info));
  raw_table->table_oid = table_oid;
  raw_table->table_name = BootstrapUtils::CopyString(table_name.c_str());
  raw_table->raw_columns = (raw_column_info **)palloc(
      sizeof(raw_column_info *) * raw_columns.size());

  oid_t column_itr = 0;
  for (auto raw_column : raw_columns) {
    raw_table->raw_columns[column_itr++] = raw_column;
  }
  raw_table->column_count = column_itr;
  return raw_table;
}

/**
 * @brief construct raw index with index oid, index name, raw columns
 * @param index oid
 * @param index name
 * @param raw columns
 * @return raw index
 */
raw_index_info *Bootstrap::GetRawIndex(
    oid_t index_oid, std::string index_name,
    std::vector<raw_column_info *> raw_columns) {
  raw_index_info *raw_index = (raw_index_info *)palloc(sizeof(raw_index_info));

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
        key_column_names.push_back(raw_column->column_name);
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

      raw_index->index_name = BootstrapUtils::CopyString(index_name.c_str());
      raw_index->index_oid = index_oid;
      raw_index->table_name =
          BootstrapUtils::CopyString(get_rel_name(pg_index->indrelid));
      raw_index->method_type = method_type;
      raw_index->constraint_type = type;
      raw_index->unique_keys = pg_index->indisunique;
      raw_index->key_column_names =
          BootstrapUtils::CopyStrings(key_column_names);
      raw_index->key_column_count = key_column_names.size();

      break;
    }
  }

  heap_endscan(pg_index_scan);
  heap_close(pg_index_rel, AccessShareLock);

  return raw_index;
}

/**
 * @brief construct raw column
 * @param relation oid
 * @param pg_attribute_rel pg_attribute catalog relation
 * @return raw columns
 */
std::vector<raw_column_info *> Bootstrap::GetRawColumn(
    Oid relation_oid, __attribute__((unused)) char relation_kind, Relation pg_attribute_rel) {
  HeapScanDesc pg_attribute_scan;
  HeapTuple pg_attribute_tuple;

  std::vector<raw_column_info *> raw_columns;

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
        std::vector<raw_constraint_info *> raw_constraints;

        PostgresValueFormat postgresValueFormat( pg_attribute->atttypid, pg_attribute->atttypmod, pg_attribute->attlen); 
        PelotonValueFormat pelotonValueFormat = FormatTransformer::TransformValueFormat(postgresValueFormat);

        ValueType value_type = pelotonValueFormat.GetType();
        int column_length = pelotonValueFormat.GetLength();
        bool is_inlined = pelotonValueFormat.IsInlined();

        // NOT NULL constraint
        if (pg_attribute->attnotnull) {
          raw_constraint_info *raw_constraint =
              (raw_constraint_info *)palloc(sizeof(raw_constraint_info));
          raw_constraint->constraint_type = CONSTRAINT_TYPE_NOTNULL;
          raw_constraint->constraint_name = nullptr;
          raw_constraint->expr = nullptr;

          raw_constraints.push_back(raw_constraint);
        }

        // DEFAULT value constraint
        if (pg_attribute->atthasdef) {
          // Setting default constraint expression
          Relation relation = heap_open(relation_oid, AccessShareLock);

          raw_constraint_info *raw_constraint =
              (raw_constraint_info *)palloc(sizeof(raw_constraint_info));
          raw_constraint->constraint_type = CONSTRAINT_TYPE_DEFAULT;
          raw_constraint->constraint_name = nullptr;
          raw_constraint->expr = nullptr;

          int num_defva = relation->rd_att->constr->num_defval;
          for (int def_itr = 0; def_itr < num_defva; def_itr++) {
            if (pg_attribute->attnum ==
                relation->rd_att->constr->defval[def_itr].adnum) {
              char *default_expression =
                  relation->rd_att->constr->defval[def_itr].adbin;
              raw_constraint->expr =
                  static_cast<Node *>(stringToNode(default_expression));
            }
          }
          //raw_constraint->constraint_name = (char *)"";
          raw_constraints.push_back(raw_constraint);

          heap_close(relation, AccessShareLock);
        }

        raw_column_info *raw_column =
            (raw_column_info *)palloc(sizeof(raw_column_info));
        raw_column->column_type = value_type;
        raw_column->column_length = column_length;
        ;
        raw_column->column_name =
            BootstrapUtils::CopyString(NameStr(pg_attribute->attname));
        raw_column->is_inlined = is_inlined;
        raw_column->raw_constraints = (raw_constraint_info **)palloc(
            sizeof(raw_constraint_info *) * raw_constraints.size());
        oid_t constraint_itr = 0;
        for (auto raw_constraint : raw_constraints) {
          raw_column->raw_constraints[constraint_itr++] = raw_constraint;
        }
        raw_column->constraint_count = constraint_itr;
        raw_constraints.clear();
        raw_columns.push_back(raw_column);
      }
    }
  }

  heap_endscan(pg_attribute_scan);

  return raw_columns;
}

void Bootstrap::GetRawForeignKeys(
    std::vector<raw_foreign_key_info *> &raw_foreignkeys) {
  Relation pg_constraint_rel;
  HeapScanDesc pg_constraint_scan;
  HeapTuple pg_constraint_tuple;

  oid_t database_oid = Bridge::GetCurrentDatabaseOid();
  assert(database_oid);

  pg_constraint_rel = heap_open(ConstraintRelationId, AccessShareLock);
  pg_constraint_scan = heap_beginscan_catalog(pg_constraint_rel, 0, NULL);

  // Go over the pg_constraint catalog table looking for foreign key constraints
  while (1) {
    Form_pg_constraint pg_constraint;

    pg_constraint_tuple =
        heap_getnext(pg_constraint_scan, ForwardScanDirection);
    if (!HeapTupleIsValid(pg_constraint_tuple)) break;

    pg_constraint = (Form_pg_constraint)GETSTRUCT(pg_constraint_tuple);

    // We only handle foreign key constraints here
    if (pg_constraint->contype != 'f') continue;

    // store raw information from here..

    raw_foreign_key_info *raw_foreignkey =
        (raw_foreign_key_info *)palloc(sizeof(raw_foreign_key_info));

    // Extract oid
    raw_foreignkey->source_table_id = pg_constraint->conrelid;
    raw_foreignkey->sink_table_id = pg_constraint->confrelid;

    // Update/Delete action
    raw_foreignkey->update_action = pg_constraint->confupdtype;
    raw_foreignkey->delete_action = pg_constraint->confdeltype;

    // TODO :: Find better way..
    // column offsets, count
    bool isNull;
    Datum curr_datum =
        heap_getattr(pg_constraint_tuple, Anum_pg_constraint_conkey,
                     RelationGetDescr(pg_constraint_rel), &isNull);
    Datum ref_datum =
        heap_getattr(pg_constraint_tuple, Anum_pg_constraint_confkey,
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

    raw_foreignkey->source_column_offsets =
        (int *)palloc(sizeof(int) * source_column_offsets.size());
    raw_foreignkey->sink_column_offsets =
        (int *)palloc(sizeof(int) * sink_column_offsets.size());

    int column_itr = 0;
    for (auto source_column_offset : source_column_offsets) {
      raw_foreignkey->source_column_offsets[column_itr++] =
          source_column_offset;
    }
    raw_foreignkey->source_column_count = source_numkeys;

    column_itr = 0;
    for (auto sink_column_offset : sink_column_offsets) {
      raw_foreignkey->sink_column_offsets[column_itr++] = sink_column_offset;
    }
    raw_foreignkey->sink_column_count = sink_numkeys;

    std::string constraint_name = NameStr(pg_constraint->conname);

    raw_foreignkey->fk_name =
        BootstrapUtils::CopyString(constraint_name.c_str());
    raw_foreignkeys.push_back(raw_foreignkey);
  }

  heap_endscan(pg_constraint_scan);
  heap_close(pg_constraint_rel, AccessShareLock);
}

void Bootstrap::CreateTables(raw_table_info **raw_tables, oid_t table_count) {
  for (int table_itr = 0; table_itr < table_count; table_itr++) {
    auto raw_table = raw_tables[table_itr];
    auto columns =
        CreateColumns(raw_table->raw_columns, raw_table->column_count);

    bool status = DDLTable::CreateTable(raw_table->table_oid,
                                        raw_table->table_name, columns);
    if (status == false) {
      elog(ERROR, "Could not create table \"%s\" in Peloton", raw_table->table_name);
    }
  }
}

void Bootstrap::CreateIndexes(raw_index_info **raw_indexes, oid_t index_count) {
  for (int index_itr = 0; index_itr < index_count; index_itr++) {
    auto raw_index = raw_indexes[index_itr];
    auto key_column_names = CreateKeyColumnNames(raw_index->key_column_names,
                                                 raw_index->key_column_count);

    IndexInfo index_info(raw_index->index_name, raw_index->index_oid,
                         raw_index->table_name, raw_index->method_type,
                         raw_index->constraint_type, raw_index->unique_keys,
                         key_column_names);

    bool status = DDLIndex::CreateIndex(index_info);

    if (status == false) {
      elog(ERROR, "Could not create index \"%s\" in Peloton", raw_index->index_name);
    }
  }
}

void Bootstrap::CreateForeignkeys(raw_foreign_key_info **raw_foreignkeys,
                                  oid_t foreignkey_count) {
  for (int foreignkey_itr = 0; foreignkey_itr < foreignkey_count;
       foreignkey_itr++) {
    auto raw_foreignkey = raw_foreignkeys[foreignkey_itr];

    // Get source table, sink table, and database oid
    oid_t source_table_oid = raw_foreignkey->source_table_id;
    assert(source_table_oid);
    oid_t sink_table_oid = raw_foreignkey->sink_table_id;
    assert(sink_table_oid);
    oid_t database_oid = Bridge::GetCurrentDatabaseOid();

    // get source, sink tables
    auto &manager = catalog::Manager::GetInstance();
    auto source_table = manager.GetTableWithOid(database_oid, source_table_oid);
    assert(source_table);
    auto sink_table = manager.GetTableWithOid(database_oid, sink_table_oid);
    assert(sink_table);

    // extract column_names
    std::vector<std::string> sink_column_names;
    std::vector<std::string> source_column_names;

    auto source_table_schema = source_table->GetSchema();
    auto sink_table_schema = sink_table->GetSchema();

    int source_column_count = raw_foreignkey->source_column_count;
    int sink_column_count = raw_foreignkey->sink_column_count;

    // Populate primary key column names
    for (int sink_key_itr = 0; sink_key_itr < sink_column_count;
         sink_key_itr++) {
      auto sink_column_offset =
          raw_foreignkey->sink_column_offsets[sink_key_itr];
      catalog::Column column =
          sink_table_schema->GetColumn(sink_column_offset - 1);
      sink_column_names.push_back(column.GetName());
    }

    // Populate source key column names
    for (int source_key_itr = 0; source_key_itr < source_column_count;
         source_key_itr++) {
      auto source_column_offset =
          raw_foreignkey->source_column_offsets[source_key_itr];
      catalog::Column column =
          source_table_schema->GetColumn(source_column_offset - 1);
      source_column_names.push_back(column.GetName());
    }

    auto foreign_key = new catalog::ForeignKey(
        sink_table_oid, sink_column_names, source_column_names,
        raw_foreignkey->update_action, raw_foreignkey->delete_action,
        raw_foreignkey->fk_name);

    source_table->AddForeignKey(foreign_key);
  }
}

std::vector<catalog::Column> Bootstrap::CreateColumns(
    raw_column_info **raw_columns, oid_t column_count) {
  std::vector<catalog::Column> columns;

  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    auto raw_column = raw_columns[column_itr];

    catalog::Column column(raw_column->column_type, raw_column->column_length,
                           raw_column->column_name, raw_column->is_inlined);

    auto constraints = CreateConstraints(raw_column->raw_constraints,
                                         raw_column->constraint_count);

    for (auto constraint : constraints) {
      column.AddConstraint(constraint);
    }
    columns.push_back(column);
  }
  return columns;
}

std::vector<std::string> Bootstrap::CreateKeyColumnNames(
    char **raw_column_names, oid_t raw_column_count) {
  std::vector<std::string> key_column_names;

  for (int column_itr = 0; column_itr < raw_column_count; column_itr++) {
    auto raw_column_name = raw_column_names[column_itr];

    key_column_names.push_back(raw_column_name);
  }
  return key_column_names;
}

std::vector<catalog::Constraint> Bootstrap::CreateConstraints(
    raw_constraint_info **raw_constraints, oid_t constraint_count) {
  std::vector<catalog::Constraint> constraints;

  for (int constraint_itr = 0; constraint_itr < constraint_count;
       constraint_itr++) {
    auto raw_constraint = raw_constraints[constraint_itr];

    std::string constraint_name;
    if(raw_constraint->constraint_name != nullptr)
      constraint_name = std::string(raw_constraint->constraint_name);

    catalog::Constraint constraint(raw_constraint->constraint_type,
                                   constraint_name,
                                   raw_constraint->expr);
    constraints.push_back(constraint);
  }
  return constraints;
}

}  // namespace bridge
}  // namespace peloton
