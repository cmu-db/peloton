//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// ddl_index.cpp
//
// Identification: src/backend/bridge/ddl/ddl_index.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <vector>
#include <thread>

#include "backend/bridge/ddl/ddl.h"
#include "backend/bridge/ddl/ddl_index.h"
#include "backend/bridge/ddl/bridge.h"
#include "backend/catalog/manager.h"
#include "backend/common/logger.h"
#include "backend/index/index.h"
#include "backend/index/index_factory.h"
#include "backend/storage/data_table.h"
#include "backend/storage/database.h"

#include "nodes/pg_list.h"

namespace peloton {
namespace bridge {

/**
 * @brief Execute the index stmt.
 * @param the parse tree
 * @param the parsetree_stack store parsetree if the table is not created yet
 * @return true if we handled it correctly, false otherwise
 */
bool DDLIndex::ExecIndexStmt(Node *parsetree,
                             std::vector<Node *> &parsetree_stack) {
  IndexStmt *Istmt = (IndexStmt *)parsetree;

  // If table has not been created yet, store it into the parsetree stack
  auto &manager = catalog::Manager::GetInstance();
  storage::Database *db =
      manager.GetDatabaseWithOid(Bridge::GetCurrentDatabaseOid());
  if (nullptr == db->GetTableWithName(Istmt->relation->relname)) {

    {
      std::lock_guard<std::mutex> lock(parsetree_stack_mutex);
      parsetree_stack.push_back(parsetree);
    }
    return true;
  }

  IndexInfo *index_info = DDLIndex::ConstructIndexInfoByParsingIndexStmt(Istmt);

  DDLIndex::CreateIndex(*index_info);
  return true;
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
bool DDLIndex::CreateIndex(IndexInfo index_info) {
  std::string index_name = index_info.GetIndexName();
  oid_t index_oid = index_info.GetOid();
  std::string table_name = index_info.GetTableName();
  IndexConstraintType index_type = index_info.GetType();
  bool unique_keys = index_info.IsUnique();
  std::vector<std::string> key_column_names = index_info.GetKeyColumnNames();

  if (index_oid == INVALID_OID) return false;
  if (index_name.empty()) return false;
  if (table_name.empty()) return false;
  if (key_column_names.size() <= 0) return false;

  // TODO: We currently only support btree as our index implementation
  // TODO : Support other types based on "type" argument
  IndexType our_index_type = INDEX_TYPE_BTREE;

  // Get the database oid and table oid
  oid_t database_oid = Bridge::GetCurrentDatabaseOid();
  assert(database_oid);

  // Get the table location from db
  auto &manager = catalog::Manager::GetInstance();
  storage::Database *db = manager.GetDatabaseWithOid(database_oid);
  storage::DataTable *data_table = db->GetTableWithName(table_name);

  catalog::Schema *tuple_schema = data_table->GetSchema();

  // Construct key schema
  std::vector<oid_t> key_columns;

  // Based on the key column info, get the oid of the given 'key' columns in the
  // tuple schema
  for (auto key_column_name : key_column_names) {
    for (oid_t tuple_schema_column_itr = 0;
         tuple_schema_column_itr < tuple_schema->GetColumnCount();
         tuple_schema_column_itr++) {
      // Get the current column info from tuple schema
      catalog::Column column_info =
          tuple_schema->GetColumn(tuple_schema_column_itr);
      // Compare Key Schema's current column name and Tuple Schema's current
      // column name
      if (key_column_name == column_info.GetName()) {
        key_columns.push_back(tuple_schema_column_itr);

        // NOTE :: Since pg_attribute doesn't have any information about primary
        // key and unique key,
        //         I try to store these information when we create an unique and
        //         primary key index
        if (index_type == INDEX_CONSTRAINT_TYPE_PRIMARY_KEY) {
          catalog::Constraint constraint(CONSTRAINT_TYPE_PRIMARY, index_name);
          tuple_schema->AddConstraint(tuple_schema_column_itr, constraint);
        } else if (index_type == INDEX_CONSTRAINT_TYPE_UNIQUE) {
          catalog::Constraint constraint(CONSTRAINT_TYPE_UNIQUE, index_name);
          constraint.SetUniqueIndexOffset(data_table->GetIndexCount());
          tuple_schema->AddConstraint(tuple_schema_column_itr, constraint);
        }
      }
    }
  }

  auto key_schema = catalog::Schema::CopySchema(tuple_schema, key_columns);
  key_schema->SetIndexedColumns(key_columns);

  // Create index metadata and physical index
  index::IndexMetadata *metadata = new index::IndexMetadata(
      index_name, index_oid, our_index_type, index_type, tuple_schema,
      key_schema, unique_keys);
  index::Index *index = index::IndexFactory::GetInstance(metadata);

  // Record the built index in the table
  data_table->AddIndex(index);

  LOG_INFO("Created index(%lu)  %s on %s.", index_oid, index_name.c_str(),
           table_name.c_str());

  return true;
}

/**
 * @brief Construct IndexInfo from a index statement
 * @param Istmt an index statement
 * @return IndexInfo
 */
IndexInfo *DDLIndex::ConstructIndexInfoByParsingIndexStmt(IndexStmt *Istmt) {
  std::string index_name;
  oid_t index_oid = Istmt->index_id;
  std::string table_name;
  IndexType method_type;
  IndexConstraintType type = INDEX_CONSTRAINT_TYPE_DEFAULT;
  std::vector<std::string> key_column_names;

  // Table name
  table_name = Istmt->relation->relname;

  // Key column names
  ListCell *entry;
  foreach (entry, Istmt->indexParams) {
    IndexElem *indexElem = static_cast<IndexElem *>(lfirst(entry));
    if (indexElem->name != NULL) {
      key_column_names.push_back(indexElem->name);
    }
  }

  // Index name and index type
  if (Istmt->idxname == NULL) {
    if (Istmt->isconstraint) {
      if (Istmt->primary) {
        index_name = table_name + "_pkey";
        type = INDEX_CONSTRAINT_TYPE_PRIMARY_KEY;
      } else if (Istmt->unique) {
        index_name = table_name;
        for (auto column_name : key_column_names) {
          index_name += "_" + column_name + "_";
        }
        index_name += "key";
        type = INDEX_CONSTRAINT_TYPE_UNIQUE;
      }
    } else {
      index_name = table_name;
      for (auto column_name : key_column_names) {
        index_name += "_" + column_name + "_";
      }
      index_name += "idx";
    }
  } else {
    index_name = Istmt->idxname;
  }

  // Index method type
  // TODO :: More access method types need
  method_type = INDEX_TYPE_BTREE;

  IndexInfo *index_info =
      new IndexInfo(index_name, index_oid, table_name, method_type, type,
                    Istmt->unique, key_column_names);
  return index_info;
}

/**
 * @brief Create the indexes using IndexInfos and add it to the table
 * @param relation_oid relation oid
 * @return true if we create all the indexes, false otherwise
 */
bool DDLIndex::CreateIndexes(std::vector<IndexInfo> &index_infos) {
  for (auto index_info : index_infos) {
    CreateIndex(index_info);
  }
  index_infos.clear();
  return true;
}

}  // namespace bridge
}  // namespace peloton
