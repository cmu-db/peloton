/*-------------------------------------------------------------------------
 *
 * create_executor.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/executor/create_executor.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/executor/create_executor.h"

#include "backend/catalog/catalog.h"
#include "backend/catalog/database.h"
#include "backend/common/logger.h"
#include "backend/common/types.h"
#include "backend/index/index_factory.h"
#include "backend/storage/data_table.h"
#include "backend/storage/table_factory.h"

#include <cassert>
#include <algorithm>

namespace nstore {
namespace executor {

// TODO: Fix function
/*
bool CreateExecutor::Execute(parser::SQLStatement *query) {

  parser::CreateStatement* stmt = (parser::CreateStatement*) query;

  // FIXME: Get default db
  catalog::Database* db = catalog::Catalog::GetInstance().GetDatabase(DEFAULT_DB_NAME);
  assert(db);
  assert(stmt->name);

  bool ret = false;
  switch(stmt->type) {
    case parser::CreateStatement::kDatabase:
      ret = CreateDatabase(stmt);
      break;
    case parser::CreateStatement::kTable:
      ret = CreateTable(db, stmt);
      break;
    case parser::CreateStatement::kIndex:
      ret = CreateIndex(db, stmt);
      break;
      // FIXME
      //         case parser::CreateStatement::kConstraint:
      //             ret = this->CreateConstraint(db, stmt);
      //             break;
    default:
      // FIXME: This needs to use our logging infrastructure
      std::cout << "Unknown create statement type : " << stmt->type << "\n";
  } // SWITCH

  return (ret);
}
*/

bool CreateExecutor::CreateDatabase(std::string db_name) {
  catalog::Database *database = catalog::Catalog::GetInstance().GetDatabase(db_name);
  if (database != nullptr) {
    LOG_ERROR("Database already exists  : %s \n", db_name.c_str());
    catalog::Catalog::GetInstance().Unlock();
    return false;
  }

  database = new catalog::Database(db_name.c_str());

  // lock catalog
  {
    catalog::Catalog::GetInstance().Lock();

    bool status = catalog::Catalog::GetInstance().AddDatabase(database);
    if (status == false) {
      LOG_ERROR("Could not create database : %s \n", db_name.c_str());
      delete database;
      catalog::Catalog::GetInstance().Unlock();
      return false;
    }

    catalog::Catalog::GetInstance().Unlock();
  }

  LOG_WARN("Created database : %s \n", db_name.c_str());
  return true;
}

// TODO: Fix function
/*
bool CreateExecutor::CreateTable(catalog::Database* db,
                                 std::string table_name,
                                 catalog::Schema* schema) {
  catalog::Table *table = nullptr;

  assert(schema);
  auto columns = schema->GetColumns();

  // Validation
  for (auto col : columns) {

    // Validate primary keys
    if (col->type == parser::ColumnDefinition::PRIMARY) {
      if (col->primary_key) {
        for (auto key : *col->primary_key)
          if (std::find(columns.begin(), columns.end(), std::string(key)) == columns.end()) {
            LOG_ERROR("Primary key :: column not in table : -%s- \n", key);
            return false;
          }
      }
    }
    // Validate foreign keys
    else if (col->type == parser::ColumnDefinition::FOREIGN) {

      // Validate source columns
      if (col->foreign_key_source) {
        for (auto key : *col->foreign_key_source)
          if (std::find(columns.begin(), columns.end(), std::string(key)) == columns.end()) {
            LOG_ERROR("Foreign key :: source column not in table : %s \n", key);
            return false;
          }
      }

      // Validate sink columns
      if (col->foreign_key_sink) {
        for (auto key : *col->foreign_key_sink) {

          catalog::Table *foreign_table = db->GetTable(col->name);
          if (foreign_table == nullptr) {
            LOG_ERROR("Foreign table does not exist  : %s \n", col->name);
            return false;
          }

          if (foreign_table->GetColumn(key) == nullptr) {
            LOG_ERROR("Foreign key :: sink column not in foreign table %s : %s\n", col->name, key);
            return false;
          }
        }
      }

    }
    // Validate normal columns
    else {
      assert(col->name);

      if (std::find(columns.begin(), columns.end(), std::string(col->name)) != columns.end()) {
        LOG_ERROR("Duplicate column name : %s \n", col->name);
        return false;
      }
      else {
        columns.push_back(std::string(col->name));
      }
    }
  } // FOR

  // Initialization
  table = db->GetTable(table_name);
  if (table != nullptr && stmt->if_not_exists) {
    LOG_ERROR("Table already exists  : %s \n", table_name);
    return false;
  }

  table = new catalog::Table(table_name);
  std::vector<catalog::ColumnInfo> physical_columns;

  oid_t offset = 0;
  oid_t constraint_id = 0;
  oid_t index_id = 0;
  std::string constraint_name;
  std::string index_name;
  bool status = false;

  // Constraints
  for (auto col : *stmt->columns) {
    if (col->type == parser::ColumnDefinition::PRIMARY) {
      auto primary_key_cols = col->primary_key;
      bool unique = col->unique;

      constraint_name = "PK_" + std::to_string(constraint_id++);
      index_name = "INDEX_" + std::to_string(index_id++);

      std::vector<catalog::Column*> columns;
      std::vector<catalog::Column*> sink_columns; // not set
      for (auto key : *primary_key_cols)
        columns.push_back(table->GetColumn(key));

      catalog::Index *index = new catalog::Index(index_name,
                                                 INDEX_TYPE_BTREE_MULTIMAP,
                                                 unique,
                                                 columns);

      catalog::Constraint *constraint = new catalog::Constraint(constraint_name,
                                                                catalog::Constraint::CONSTRAINT_TYPE_PRIMARY,
                                                                index,
                                                                nullptr,
                                                                columns,
                                                                sink_columns);

      status = table->AddConstraint(constraint);
      if (status == false) {
        LOG_ERROR("Could not create constraint : %s \n", constraint->GetName().c_str());
        delete index;
        delete constraint;
        delete table;
        return false;
      }

      status = table->AddIndex(index);
      if (status == false) {
        LOG_ERROR("Could not create index : %s \n", index->GetName().c_str());
        delete index;
        delete constraint;
        delete table;
        return false;
      }

    }
    // Foreign Keys
    else if (col->type == parser::ColumnDefinition::FOREIGN ) {
      auto foreign_key_source_cols = col->foreign_key_source;
      auto foreign_key_sink_cols = col->foreign_key_sink;

      constraint_name = "FK_" + std::to_string(constraint_id++);

      std::vector<catalog::Column*> source_columns;
      std::vector<catalog::Column*> sink_columns;
      catalog::Table *foreign_table = db->GetTable(col->name);

      for (auto key : *foreign_key_source_cols)
        source_columns.push_back(table->GetColumn(key));
      for (auto key : *foreign_key_sink_cols)
        sink_columns.push_back(foreign_table->GetColumn(key));

      catalog::Constraint *constraint = new catalog::Constraint(constraint_name,
                                                                catalog::Constraint::CONSTRAINT_TYPE_FOREIGN,
                                                                nullptr,
                                                                foreign_table,
                                                                source_columns,
                                                                sink_columns);

      status = table->AddConstraint(constraint);
      if (status == false) {
        LOG_ERROR("Could not create constraint : %s \n", constraint->GetName().c_str());
        delete constraint;
        delete table;
        return false;
      }
    }
    // Normal(?) Columns
    else {

      ValueType type = parser::ColumnDefinition::GetValueType(col->type);
      size_t col_len = GetTypeSize(type);

      if (col->type == parser::ColumnDefinition::CHAR)
        col_len = 1;
      if (type == VALUE_TYPE_VARCHAR || type == VALUE_TYPE_VARBINARY)
        col_len = col->varlen;

      catalog::Column *column = new catalog::Column(col->name,
                                                    type,
                                                    offset++,
                                                    col_len,
                                                    col->not_null);

      bool varlen = false;
      if (col->varlen != 0)
        varlen = true;

      catalog::ColumnInfo physical_column(type, col_len, col->name, !col->not_null, varlen);
      physical_columns.push_back(physical_column);

      status = table->AddColumn(column);
      if (status == false) {
        LOG_ERROR("Could not create column : %s \n", column->GetName().c_str());
        delete table;
        return false;
      }

    }
  } // FOR

  // In-Memory Data Table
  catalog::Schema *schema = new catalog::Schema(physical_columns);
  storage::DataTable *physical_table = storage::TableFactory::GetDataTable(DEFAULT_DB_ID, schema, table_name);
  table->SetPhysicalTable(physical_table);

  // lock database
  {
    db->Lock();

    bool status = db->AddTable(table);
    if (status == false) {
      LOG_ERROR("Could not create table : %s \n", table_name);
      delete table;
      db->Unlock();
      return false;
    }

    db->Unlock();
  }

  LOG_WARN("Created table : %s \n", table_name);
  return true;
}
*/

bool CreateExecutor::CreateIndex(catalog::Database* db,
                                 std::string index_name,
                                 std::string table_name,
                                 std::vector<std::string> index_attrs,
                                 bool unique) {
  catalog::Table *table = db->GetTable(table_name);
  if (table == nullptr) {
    LOG_ERROR("Table does not exist  : %s \n", table_name.c_str());
    return false;
  }

  if (index_attrs.empty()) {
    LOG_ERROR("No index attributes defined for index : %s \n", index_name.c_str());
    return false;
  }

  std::vector<oid_t> key_attrs;
  std::vector<catalog::Column*> table_columns = table->GetColumns();
  std::vector<catalog::Column*> key_columns;

  for (auto key : index_attrs) {
    catalog::Column *column = table->GetColumn(key);
    if (column == nullptr) {
      LOG_ERROR("Index attribute does not exist in table : %s %s \n", key.c_str(), table_name.c_str());
      return false;
    }
    else {
      key_attrs.push_back(column->GetOffset());
      key_columns.push_back(column);
    }
  }

  //===--------------------------------------------------------------------===//
  // Physical index
  //===--------------------------------------------------------------------===//

  auto tuple_schema = table->GetTable()->GetSchema();
  catalog::Schema *key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);

  index::IndexMetadata *index_metadata = new index::IndexMetadata(index_name,
                                                                  INDEX_TYPE_BTREE_MULTIMAP,
                                                                  tuple_schema,
                                                                  key_schema,
                                                                  unique);

  index::Index *physical_index = index::IndexFactory::GetInstance(index_metadata);

  catalog::Index *index = new catalog::Index(index_name,
                                             INDEX_TYPE_BTREE_MULTIMAP,
                                             unique,
                                             key_columns);

  // lock table
  {
    table->Lock();

    bool status = table->AddIndex(index);
    if (status == false) {
      LOG_ERROR("Could not create index : %s \n", index_name.c_str());
      delete physical_index;
      delete index;
      delete index_metadata;
      table->Unlock();
      return false;
    }

    table->Unlock();
  }

  index->SetPhysicalIndex(physical_index);

  LOG_WARN("Created index : %s \n", index_name.c_str());

  return true;
}

bool CreateExecutor::CreateConstraint(catalog::Database* db,
                                      std::string table_name) {

  catalog::Table *table = db->GetTable(table_name);
  if (table == nullptr) {
    LOG_ERROR("Table does not exist  : %s \n", table_name.c_str());
    return false;
  }

  // TODO : Need to work on this.

  return (true);
}


} // namespace executor
} // namespace nstore

