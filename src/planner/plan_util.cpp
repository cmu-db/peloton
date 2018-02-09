//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// plan_util.cpp
//
// Identification: src/planner/plan_util.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <set>
#include <string>

#include "catalog/catalog_cache.h"
#include "catalog/column_catalog.h"
#include "catalog/database_catalog.h"
#include "catalog/index_catalog.h"
#include "catalog/table_catalog.h"
#include "parser/delete_statement.h"
#include "parser/insert_statement.h"
#include "parser/sql_statement.h"
#include "parser/update_statement.h"
#include "planner/plan_util.h"
#include "util/set_util.h"

namespace peloton {
namespace planner {

const std::set<oid_t> PlanUtil::GetAffectedIndexes(
    catalog::CatalogCache &catalog_cache,
    const parser::SQLStatement &sql_stmt) {
  std::set<oid_t> index_oids;
  std::string db_name, table_name;
  switch (sql_stmt.GetType()) {
    // For INSERT, DELETE, all indexes are affected
    case StatementType::INSERT: {
      auto &insert_stmt =
          static_cast<const parser::InsertStatement &>(sql_stmt);
      db_name = insert_stmt.GetDatabaseName();
      table_name = insert_stmt.GetTableName();
    }
      PELOTON_FALLTHROUGH;
    case StatementType::DELETE: {
      if (table_name.empty() || db_name.empty()) {
        auto &delete_stmt =
            static_cast<const parser::DeleteStatement &>(sql_stmt);
        db_name = delete_stmt.GetDatabaseName();
        table_name = delete_stmt.GetTableName();
      }
      auto indexes_map = catalog_cache.GetDatabaseObject(db_name)
                             ->GetTableObject(table_name)
                             ->GetIndexObjects();
      for (auto &index : indexes_map) {
        index_oids.insert(index.first);
      }
    } break;
    case StatementType::UPDATE: {
      auto &update_stmt =
          static_cast<const parser::UpdateStatement &>(sql_stmt);
      db_name = update_stmt.table->GetDatabaseName();
      table_name = update_stmt.table->GetTableName();
      auto db_object = catalog_cache.GetDatabaseObject(db_name);
      auto table_object = db_object->GetTableObject(table_name);

      auto &update_clauses = update_stmt.updates;
      std::set<oid_t> update_oids;
      for (const auto &update_clause : update_clauses) {
        LOG_TRACE("Affected column name for table(%s) in UPDATE query: %s",
                  table_name.c_str(), update_clause->column.c_str());
        auto col_object = table_object->GetColumnObject(update_clause->column);
        update_oids.insert(col_object->GetColumnId());
      }

      auto indexes_map = table_object->GetIndexObjects();
      for (auto &index : indexes_map) {
        LOG_TRACE("Checking if UPDATE query affects index: %s",
                  index.second->GetIndexName().c_str());
        const std::vector<oid_t> &key_attrs =
            index.second->GetKeyAttrs();  // why it's a vector, and not set?
        const std::set<oid_t> key_attrs_set(key_attrs.begin(), key_attrs.end());
        if (!SetUtil::IsDisjoint(key_attrs_set, update_oids)) {
          LOG_TRACE("Index (%s) is affected",
                    index.second->GetIndexName().c_str());
          index_oids.insert(index.first);
        }
      }
    } break;
    case StatementType::SELECT:
      break;
    default:
      LOG_TRACE("Does not support finding affected indexes for query type: %d",
                static_cast<int>(sql_stmt.GetType()));
  }
  return (index_oids);
}

}  // namespace planner
}  // namespace peloton
