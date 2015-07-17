/*-------------------------------------------------------------------------
 *
 * ddl_create.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/bridge/ddl_create.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <cassert>
#include <iostream>

#include "backend/bridge/ddl.h"
#include "backend/bridge/bridge.h"
#include "backend/catalog/schema.h"
#include "backend/common/logger.h"
#include "backend/common/types.h"
#include "backend/index/index.h"
#include "backend/index/index_factory.h"
#include "backend/storage/backend_vm.h"
#include "backend/storage/table_factory.h"
#include "backend/storage/database.h"

#include "postgres.h"
#include "miscadmin.h"
#include "c.h"
#include "nodes/parsenodes.h"
#include "parser/parse_utilcmd.h"
#include "parser/parse_type.h"
#include "access/htup_details.h"
#include "utils/resowner.h"
#include "utils/syscache.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"

#include "parser/parse_node.h" // make pstate cook default
#include "parser/parse_expr.h" // cook default

namespace peloton {
namespace bridge {

static std::vector<DDL::IndexInfo> index_infos;

//===--------------------------------------------------------------------===//
// Create Object
//===--------------------------------------------------------------------===//

/**
 * @brief Create database.
 * @param database_oid database id
 * @return true if we created a database, false otherwise
 */
bool DDL::CreateDatabase(Oid database_oid){

  auto& manager = catalog::Manager::GetInstance();
  storage::Database* db = manager.GetDatabaseWithOid(Bridge::Bridge::GetCurrentDatabaseOid());

  if(db == nullptr){
    LOG_WARN("Failed to create a database (%u)", database_oid);
    return false;
  }

  LOG_INFO("Create database (%u)", database_oid);
  return true;
}

/**
 * @brief Create table.
 * @param table_name Table name
 * @param column_infos Information about the columns
 * @param schema Schema for the table
 * @return true if we created a table, false otherwise
 */
bool DDL::CreateTable(Oid relation_oid,
                       std::string table_name,
                       std::vector<catalog::Column> column_infos,
                       catalog::Schema *schema){

  assert(!table_name.empty());

  Oid database_oid = Bridge::GetCurrentDatabaseOid();
  if(database_oid == INVALID_OID || relation_oid == INVALID_OID)
    return false;

  // Get db oid
  auto& manager = catalog::Manager::GetInstance();
  storage::Database* db = manager.GetDatabaseWithOid(database_oid);

  // Construct our schema from vector of ColumnInfo
  if(schema == NULL) 
    schema = new catalog::Schema(column_infos);

  // Build a table from schema
  storage::DataTable *table = storage::TableFactory::GetDataTable(database_oid, relation_oid, schema, table_name);

  db->AddTable(table);

  if(table != nullptr) {
    LOG_INFO("Created table(%u) : %s", relation_oid, table_name.c_str());
    return true;
  }else{
    LOG_WARN("Failed to create a table(%u) : %s", relation_oid, table_name.c_str());
  }

  return false;
}


/**
 * @brief Create index.
 * @param index_name Index name
 * @param table_name Table name
 * @param type Type of the index
 * @param unique Index is unique or not ?
 * @param key_column_names column names for the key table 
 * @return true if we create the index, false otherwise
 */
bool DDL::CreateIndex(IndexInfo index_info){

  std::string index_name = index_info.GetIndexName();
  oid_t index_oid = index_info.GetOid();
  std::string table_name = index_info.GetTableName();
  IndexConstraintType index_type = index_info.GetType();
  bool unique_keys = index_info.IsUnique();
  std::vector<std::string> key_column_names = index_info.GetKeyColumnNames();

  assert(!index_name.empty());
  assert(!table_name.empty());
  assert(key_column_names.size() > 0);

  // TODO: We currently only support btree as our index implementation
  // TODO : Support other types based on "type" argument
  IndexType our_index_type = INDEX_TYPE_BTREE_MULTIMAP;

  // Get the database oid and table oid
  oid_t database_oid = Bridge::GetCurrentDatabaseOid();
  assert(database_oid);

  // Get the table location from db
  auto& manager = catalog::Manager::GetInstance();
  storage::Database* db = manager.GetDatabaseWithOid(database_oid);
  storage::DataTable* data_table = db->GetTableWithName(table_name);

  catalog::Schema *tuple_schema = data_table->GetSchema();

  // Construct key schema
  std::vector<oid_t> key_columns;

  // Based on the key column info, get the oid of the given 'key' columns in the tuple schema
  for(auto key_column_name : key_column_names){
    for(oid_t  tuple_schema_column_itr = 0; tuple_schema_column_itr < tuple_schema->GetColumnCount();
        tuple_schema_column_itr++){

      // Get the current column info from tuple schema
      catalog::Column column_info = tuple_schema->GetColumn(tuple_schema_column_itr);
      // Compare Key Schema's current column name and Tuple Schema's current column name
      if(key_column_name == column_info.GetName()){
        key_columns.push_back(tuple_schema_column_itr);

        // NOTE :: Since pg_attribute doesn't have any information about primary key and unique key,
        //         I try to store these information when we create an unique and primary key index
        if(index_type == INDEX_CONSTRAINT_TYPE_PRIMARY_KEY){ 
          catalog::Constraint constraint(CONSTRAINT_TYPE_PRIMARY, index_name);
          tuple_schema->AddConstraint(tuple_schema_column_itr, constraint);
        }else if(index_type == INDEX_CONSTRAINT_TYPE_UNIQUE){ 
          catalog::Constraint constraint(CONSTRAINT_TYPE_UNIQUE, index_name);
          constraint.SetUniqueIndexOffset(data_table->GetIndexCount());
          tuple_schema->AddConstraint(tuple_schema_column_itr, constraint);
        }

      }
    }
  }

  auto key_schema = catalog::Schema::CopySchema(tuple_schema, key_columns);

  // Create index metadata and physical index
  index::IndexMetadata* metadata = new index::IndexMetadata(index_name, index_oid, our_index_type, index_type,
                                                            tuple_schema, key_schema,
                                                            unique_keys);
  index::Index* index = index::IndexFactory::GetInstance(metadata);

  // Record the built index in the table
  data_table->AddIndex(index);

  LOG_INFO("Create index %s on %s.", index_name.c_str(), table_name.c_str());

  return true;
}

} // namespace bridge
} // namespace peloton
