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

bool CreateExecutor::Execute(parser::SQLStatement *query) {

  // Handle DDL statements
  parser::CreateStatement* stmt = (parser::CreateStatement*) query;

  // Get default db
  catalog::Database* db = catalog::Catalog::GetInstance().GetDatabase("default");

  switch(stmt->type) {
    case parser::CreateStatement::kTable: {
      assert(db);
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

                catalog::Table *foreign_table = db->GetTable(col->name);
                if(foreign_table == nullptr) {
                  LOG_ERROR("Foreign table does not exist  : %s \n", col->name);
                  return false;
                }

                if(foreign_table->GetColumn(key) == nullptr){
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

      catalog::Table *table = db->GetTable(stmt->name);
      if(table != nullptr && stmt->if_not_exists) {
        LOG_ERROR("Table already exists  : %s \n", stmt->name);
        return false;
      }

      // Setup table
      oid_t offset = 0;
      for(auto col : *stmt->columns) {
        catalog::Column *column;

        // Create primary keys
        if(col->type == parser::ColumnDefinition::PRIMARY) {

          column = table->GetColumn(col->name);

          //catalog::Constraint *constraint = new catalog::Constraint("PK");
          //column->AddConstraint(constraint);

          // setup index ??
        }
        // Create foreign keys
        else if(col->type == parser::ColumnDefinition::FOREIGN ) {

        }
        // Create normal columns
        else {

          ValueType type = parser::ColumnDefinition::GetValueType(col->type);
          size_t col_len = GetTypeSize(type);
          if(col->type == parser::ColumnDefinition::CHAR)
            col_len = 1;
          if(type == VALUE_TYPE_VARCHAR || type == VALUE_TYPE_VARBINARY)
            col_len = col->varlen;

          catalog::Column *column = new catalog::Column(col->name,
                                                        type,
                                                        offset++,
                                                        col_len,
                                                        !col->not_null);

          column = table->AddColumn(column);
        }
      }

      LOG_WARN("Created table : %s \n", stmt->name);
    }
    break;

    case parser::CreateStatement::kDatabase: {

      catalog::Database *database = catalog::Catalog::GetInstance().GetDatabase(stmt->name);
      if(database != nullptr) {
        LOG_ERROR("Database already exists  : %s \n", stmt->name);
        return false;
      }

      database = new catalog::Database(stmt->name);
      catalog::Catalog::GetInstance().AddDatabase(database);

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

