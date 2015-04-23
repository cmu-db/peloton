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

#include "catalog/catalog.h"
#include "executor/create_executor.h"
#include "parser/statement_create.h"
#include "common/logger.h"
#include "catalog/database.h"

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

    //===--------------------------------------------------------------------===//
    // TABLE
    //===--------------------------------------------------------------------===//

    case parser::CreateStatement::kTable: {
      catalog::Table *table = nullptr;

      assert(db);
      assert(stmt->name);
      assert(stmt->columns);

      // Column names
      std::vector<std::string> columns;
      for(auto col : *stmt->columns) {

        //===--------------------------------------------------------------------===//
        // Validation
        //===--------------------------------------------------------------------===//

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

      //===--------------------------------------------------------------------===//
      // Setup table
      //===--------------------------------------------------------------------===//

      table = db->GetTable(stmt->name);
      if(table != nullptr && stmt->if_not_exists) {
        LOG_ERROR("Table already exists  : %s \n", stmt->name);
        return false;
      }

      table = new catalog::Table(stmt->name);

      oid_t offset = 0;
      oid_t constraint_id = 0;
      oid_t index_id = 0;
      std::string constraint_name;
      std::string index_name;
      bool status = false;

      for(auto col : *stmt->columns) {

        //===--------------------------------------------------------------------===//
        // Create primary keys
        //===--------------------------------------------------------------------===//
        if(col->type == parser::ColumnDefinition::PRIMARY) {

          auto primary_key_cols = col->primary_key;
          bool unique = col->unique;

          constraint_name = "PK_" + std::to_string(constraint_id++);
          index_name = "INDEX_" + std::to_string(index_id++);

          std::vector<catalog::Column*> columns;
          std::vector<catalog::Column*> sink_columns; // not set
          for(auto key : *primary_key_cols)
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
          if(status == false) {
            LOG_ERROR("Could not create constraint : %s \n", constraint->GetName().c_str());
            delete index;
            delete constraint;
            delete table;
            return false;
          }

          status = table->AddIndex(index);
          if(status == false) {
            LOG_ERROR("Could not create index : %s \n", index->GetName().c_str());
            delete index;
            delete constraint;
            delete table;
            return false;
          }

        }
        //===--------------------------------------------------------------------===//
        // Create foreign keys
        //===--------------------------------------------------------------------===//
        else if(col->type == parser::ColumnDefinition::FOREIGN ) {

          auto foreign_key_source_cols = col->foreign_key_source;
          auto foreign_key_sink_cols = col->foreign_key_sink;

          constraint_name = "FK_" + std::to_string(constraint_id++);

          std::vector<catalog::Column*> source_columns;
          std::vector<catalog::Column*> sink_columns;
          catalog::Table *foreign_table = db->GetTable(col->name);

          for(auto key : *foreign_key_source_cols)
            source_columns.push_back(table->GetColumn(key));
          for(auto key : *foreign_key_sink_cols)
            sink_columns.push_back(foreign_table->GetColumn(key));

          catalog::Constraint *constraint = new catalog::Constraint(constraint_name,
                                                                    catalog::Constraint::CONSTRAINT_TYPE_FOREIGN,
                                                                    nullptr,
                                                                    foreign_table,
                                                                    source_columns,
                                                                    sink_columns);

          status = table->AddConstraint(constraint);
          if(status == false) {
            LOG_ERROR("Could not create constraint : %s \n", constraint->GetName().c_str());
            delete constraint;
            delete table;
            return false;
          }

        }
        //===--------------------------------------------------------------------===//
        // Create normal columns
        //===--------------------------------------------------------------------===//
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
                                                        col->not_null);

          bool status = table->AddColumn(column);
          if(status == false) {
            LOG_ERROR("Could not create column : %s \n", column->GetName().c_str());
            delete table;
            return false;
          }

        }
      }

      db->AddTable(table);
      LOG_WARN("Created table : %s \n", stmt->name);

    }
    break;

    //===--------------------------------------------------------------------===//
    // DATABASE
    //===--------------------------------------------------------------------===//

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

    //===--------------------------------------------------------------------===//
    // INDEX
    //===--------------------------------------------------------------------===//

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

