/*-------------------------------------------------------------------------
 *
 * drop_executor.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/executor/drop_executor.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "executor/drop_executor.h"

#include "catalog/catalog.h"
#include "catalog/database.h"
#include "common/logger.h"
#include "common/types.h"

#include "index/index.h"
#include "storage/table.h"
#include "parser/statement_drop.h"

#include <cassert>
#include <algorithm>

namespace nstore {
namespace executor {

bool DropExecutor::Execute(parser::SQLStatement *query) {

  parser::DropStatement* stmt = (parser::DropStatement*) query;

  // Get default db
  catalog::Database* db = catalog::Catalog::GetInstance().GetDatabase(DEFAULT_DB_NAME);
  assert(db);
  assert(stmt->name);

  bool status;

  switch(stmt->type) {

    //===--------------------------------------------------------------------===//
    // TABLE
    //===--------------------------------------------------------------------===//
    case parser::DropStatement::EntityType::kTable: {

      catalog::Table *table = db->GetTable(stmt->name);
      if(table == nullptr) {
        LOG_ERROR("Table does not exist  : %s \n", stmt->name);
        return false;
      }

      status = db->RemoveTable(stmt->name);
      if(status == false) {
        LOG_ERROR("Could not drop table : %s \n", stmt->name);
        return false;
      }

      // this will also clean the underlying physical table
      delete table;
      LOG_WARN("Dropped table : %s \n", stmt->name);
      return true;
    }
    break;

    //===--------------------------------------------------------------------===//
    // DATABASE
    //===--------------------------------------------------------------------===//
    case parser::DropStatement::EntityType::kDatabase: {

      catalog::Database *database = catalog::Catalog::GetInstance().GetDatabase(stmt->name);
      if(database == nullptr) {
        LOG_ERROR("Database does not exist  : %s \n", stmt->name);
        return false;
      }

      status = catalog::Catalog::GetInstance().RemoveDatabase(stmt->name);
      if(status == false) {
        LOG_ERROR("Could not drop database : %s \n", stmt->name);
        return false;
      }

      // clean up database
      delete database;
      LOG_WARN("Dropped database : %s \n", stmt->name);
      return true;
    }
    break;

    //===--------------------------------------------------------------------===//
    // INDEX
    //===--------------------------------------------------------------------===//
    case parser::DropStatement::EntityType::kIndex: {
      assert(stmt->table_name);

      catalog::Table *table = db->GetTable(stmt->table_name);
      if(table == nullptr) {
        LOG_ERROR("Table does not exist  : %s \n", stmt->table_name);
        return false;
      }

      catalog::Index *index = table->GetIndex(stmt->name);
      if(index == nullptr) {
        LOG_ERROR("Index does not exist  : %s \n", stmt->name);
        return false;
      }

      status = table->RemoveIndex(stmt->name);
      if(status == false) {
        LOG_ERROR("Could not drop index on table : %s %s \n", stmt->name, stmt->table_name);
        return false;
      }

      // clean up index
      delete index;
      LOG_WARN("Dropped index : %s \n", stmt->name);
      std::cout << (*db);
      return true;
    }
    break;

    default:
      std::cout << "Unknown drop statement type : " << stmt->type << "\n";
      break;
  }

  return false;
}

} // namespace executor
} // namespace nstore


