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

#include "backend/executor/drop_executor.h"

#include "backend/catalog/catalog.h"
#include "backend/catalog/database.h"
#include "backend/common/logger.h"
#include "backend/common/types.h"

#include "backend/index/index.h"
#include "backend/storage/data_table.h"

#include <cassert>
#include <algorithm>

namespace peloton {
namespace executor {

// TODO: Fix function
static bool Execute();
/*
bool DropExecutor::Execute() {

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

      // lock database
      {
        db->Lock();

        status = db->RemoveTable(stmt->name);
        if(status == false) {
          LOG_ERROR("Could not drop table : %s \n", stmt->name);
          db->Unlock();
          return false;
        }

        db->Unlock();
      }

      // clean up physical table
      delete table->GetPhysicalTable();

      // clean up catalog table
      delete table;
      LOG_WARN("Dropped table : %s \n", stmt->name);
      std::cout << (*db);

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
        catalog::Catalog::GetInstance().Unlock();
        return false;
      }

      // lock catalog
      {
        catalog::Catalog::GetInstance().Lock();

        status = catalog::Catalog::GetInstance().RemoveDatabase(stmt->name);
        if(status == false) {
          LOG_ERROR("Could not drop database : %s \n", stmt->name);
          catalog::Catalog::GetInstance().Unlock();
          return false;
        }

        catalog::Catalog::GetInstance().Unlock();
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

      // lock table
      {
        table->Lock();

        status = table->RemoveIndex(stmt->name);
        if(status == false) {
          LOG_ERROR("Could not drop index on table : %s %s \n", stmt->name, stmt->table_name);
          table->Unlock();
          return false;
        }

        table->Unlock();
      }

      // clean up physical index
      delete index->GetPhysicalIndex();

      // clean up catalog index
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
*/

} // namespace executor
} // namespace peloton


