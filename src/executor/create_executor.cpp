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

#include "executor/create_executor.h"
#include "parser/statement_create.h"
#include "common/logger.h"

#include <cassert>
#include <algorithm>

namespace nstore {
namespace executor {

bool CreateExecutor::has_default_db = false;
catalog::CatalogType *CreateExecutor::cluster = nullptr;

bool CreateExecutor::Execute(parser::SQLStatement *query) {

  // Handle DML statements
  parser::CreateStatement* stmt = (parser::CreateStatement*) query;

  // Setup default database

  if(CreateExecutor::has_default_db == false) {
    cluster = catalog::Catalog::GetInstance().AddChild("clusters", "default");
    assert(cluster);
    cluster->AddChild("databases", "default");
    CreateExecutor::has_default_db = true;
  }

  switch(stmt->type) {
    case parser::CreateStatement::kTable: {
      catalog::CatalogType *database = cluster->GetChild("databases", "default");
      assert(database);
      assert(stmt->name);
      assert(stmt->columns);

      // Column names
      std::vector<std::string> columns;
      for(auto col : *stmt->columns) {

        // Validate primary keys
        if(col->type == parser::ColumnDefinition::PRIMARY) {
          if(col->primary_key) {
            for(auto key : *col->primary_key)
              if(std::find(columns.begin(), columns.end(), std::string(key)) == columns.end()) {
                LOG_ERROR("Primary key :: column not in table : -%s- \n", key);
                return false;
              }
          }
        }
        // Validate foreign keys
        else if(col->type == parser::ColumnDefinition::FOREIGN ) {

            // Validate source columns
            if(col->foreign_key_source) {
              for(auto key : *col->foreign_key_source)
                if(std::find(columns.begin(), columns.end(), std::string(key)) == columns.end()) {
                  LOG_ERROR("Foreign key :: source column not in table : %s \n", key);
                  return false;
                }
            }

            // Validate sink columns
            if(col->foreign_key_sink) {
              for(auto key : *col->foreign_key_sink) {

                catalog::CatalogType *foreign_table = database->GetChild("tables", col->name);
                if(foreign_table == nullptr) {
                  LOG_ERROR("Foreign table does not exist  : %s \n", col->name);
                  return false;
                }

                if(foreign_table->GetChild("columns", key) == nullptr){
                  LOG_ERROR("Foreign key :: sink column not in foreign table %s : %s\n", col->name, key);
                  return false;
                }
              }
            }

        }
        // Validate normal columns
        else {
          assert(col->name);

          if(std::find(columns.begin(), columns.end(), std::string(col->name)) != columns.end()) {
            LOG_ERROR("Duplicate column name : %s \n", col->name);
            return false;
          }
          else {
            columns.push_back(std::string(col->name));
          }

        }
      }

      catalog::CatalogType *table = database->AddChild("tables", std::string(stmt->name));
      if(table == nullptr && stmt->if_not_exists) {
        LOG_ERROR("Table already exists  : %s \n", stmt->name);
        return false;
      }

      // Setup table
      for(auto col : *stmt->columns) {
        catalog::CatalogType *column;

        // Create primary keys
        if(col->type == parser::ColumnDefinition::PRIMARY) {

          column = table->GetChild("columns", std::string(col->name));
          catalog::CatalogType *constraint = column->AddChild("constraints", "PK");
          constraint->Set("index", "index1");

          // setup index ??
        }
        // Create foreign keys
        else if(col->type == parser::ColumnDefinition::FOREIGN ) {

        }
        // Create normal columns
        else {
          column = table->AddChild("columns", std::string(col->name));

          ValueType type = parser::ColumnDefinition::GetValueType(col->type);
          size_t col_len = GetTypeSize(type);
          if(col->type == parser::ColumnDefinition::CHAR)
            col_len = 1;
          if(type == VALUE_TYPE_VARCHAR || type == VALUE_TYPE_VARBINARY)
            col_len = col->varlen;

          column->Set("index", "0");
          column->Set("type", std::to_string(type));
          column->Set("size", std::to_string(col_len));
          column->Set("nullable", std::to_string(!col->not_null));
        }
      }

      LOG_WARN("Created table : %s \n", stmt->name);
    }
    break;

    case parser::CreateStatement::kDatabase: {
      catalog::CatalogType *database = cluster->AddChild("databases", std::string(stmt->name));

      if(database == nullptr) {
        LOG_ERROR("Database already exists  : %s \n", stmt->name);
        return false;
      }

      LOG_WARN("Created database : %s \n", stmt->name);
    }
    break;

    case parser::CreateStatement::kIndex:
      break;

    default:
      std::cout << "Unknown create statement type : " << stmt->type << "\n";
      break;
  }


  return false;
}

} // namespace executor
} // namespace nstore

