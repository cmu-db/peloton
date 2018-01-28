//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// plan_util.h
//
// Identification: src/include/planner/plan_util.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <set>
#include <string>

#include "catalog/catalog_cache.h"
#include "catalog/database_catalog.h"
#include "catalog/table_catalog.h"
#include "catalog/column_catalog.h"
#include "catalog/index_catalog.h"

#include "parser/delete_statement.h"
#include "parser/insert_statement.h"
#include "parser/sql_statement.h"
#include "parser/update_statement.h"

#include "planner/abstract_plan.h"
#include "planner/abstract_scan_plan.h"
#include "planner/delete_plan.h"
#include "planner/insert_plan.h"
#include "planner/update_plan.h"
#include "planner/populate_index_plan.h"
#include "storage/data_table.h"
#include "util/string_util.h"

namespace peloton {
namespace planner {

class PlanUtil {
 public:
  /**
   * @brief Pretty print the plan tree.
   * @param The plan tree
   * @return The string with the pretty-print plan
   */
  static std::string GetInfo(const planner::AbstractPlan *plan);

  /**
   * @brief Get the tables referenced in the plan
   * @param The plan tree
   * @return set of the Table oids
   */
  static const std::set<oid_t> GetTablesReferenced(
      const planner::AbstractPlan *plan);

  /**
  * @brief Get the indexes affected by a given query
  * @param CatalogCache
  * @param SQLStatement
  * @return set of affected index object ids
  */
  static const std::set<oid_t> GetAffectedIndexes(
      catalog::CatalogCache& catalog_cache, parser::SQLStatement* sql_stmt);

 private:
  ///
  /// Helpers for GetInfo() and GetTablesReferenced()
  ///

  static void GetInfo(const planner::AbstractPlan *plan, std::ostringstream &os,
                      int num_indent);

  static void GetTablesReferenced(const planner::AbstractPlan *plan,
                                  std::set<oid_t> &table_ids);
};

////////////////////////////////////////////////////////////////////////////////
///
/// Implementation below
///
////////////////////////////////////////////////////////////////////////////////

inline std::string PlanUtil::GetInfo(const planner::AbstractPlan *plan) {
  std::ostringstream os;
  int num_indent = 0;

  if (plan == nullptr) {
    os << "<NULL>";
  } else {
    os << peloton::GETINFO_SINGLE_LINE << std::endl;
    PlanUtil::GetInfo(plan, os, num_indent);
    os << peloton::GETINFO_SINGLE_LINE;
  }
  std::string info = os.str();
  StringUtil::RTrim(info);
  return info;
}

inline void PlanUtil::GetInfo(const planner::AbstractPlan *plan,
                              std::ostringstream &os, int num_indent) {
  os << StringUtil::Indent(num_indent)
     << "-> Plan Type: " << PlanNodeTypeToString(plan->GetPlanNodeType())
     << std::endl;
  os << StringUtil::Indent(num_indent + peloton::ARROW_INDENT)
     << "Info: " << plan->GetInfo() << std::endl;

  auto &children = plan->GetChildren();
  os << StringUtil::Indent(num_indent + peloton::ARROW_INDENT)
     << "NumChildren: " << children.size() << std::endl;
  for (auto &child : children) {
    GetInfo(child.get(), os, num_indent + peloton::ARROW_INDENT);
  }
}

inline const std::set<oid_t> PlanUtil::GetTablesReferenced(
    const planner::AbstractPlan *plan) {
  std::set<oid_t> table_ids;
  if (plan != nullptr) {
    GetTablesReferenced(plan, table_ids);
  }
  return (table_ids);
}

inline void PlanUtil::GetTablesReferenced(const planner::AbstractPlan *plan,
                                          std::set<oid_t> &table_ids) {
  switch (plan->GetPlanNodeType()) {
    case PlanNodeType::SEQSCAN:
    case PlanNodeType::INDEXSCAN: {
      const auto *scan_node =
          reinterpret_cast<const planner::AbstractScan *>(plan);
      table_ids.insert(scan_node->GetTable()->GetOid());
      break;
    }
    case PlanNodeType::INSERT: {
      const auto *insert_node =
          reinterpret_cast<const planner::InsertPlan *>(plan);
      table_ids.insert(insert_node->GetTable()->GetOid());
      break;
    }
    case PlanNodeType::UPDATE: {
      const auto *update_node =
          reinterpret_cast<const planner::UpdatePlan *>(plan);
      table_ids.insert(update_node->GetTable()->GetOid());
      break;
    }
    case PlanNodeType::DELETE: {
      const auto *delete_node =
          reinterpret_cast<const planner::DeletePlan *>(plan);
      table_ids.insert(delete_node->GetTable()->GetOid());
      break;
    }
    case PlanNodeType::POPULATE_INDEX: {
      const auto *populate_index_node =
          reinterpret_cast<const planner::PopulateIndexPlan *>(plan);
      table_ids.insert(populate_index_node->GetTable()->GetOid());
      break;
    }
    default: {
      // Nothing to do, nothing to see...
      break;
    }
  }  // SWITCH
  for (auto &child : plan->GetChildren()) {
    if (child != nullptr) {
      GetTablesReferenced(child.get(), table_ids);
    }
  }
}

namespace {

template <typename T>
bool isDisjoint(const std::set<T> &setA, const std::set<T> &setB) {
  bool disjoint = true;
  if (setA.empty() || setB.empty())
    return disjoint;

  typename std::set<T>::const_iterator setA_it = setA.begin();
  typename std::set<T>::const_iterator setB_it = setB.begin();
  while (setA_it != setA.end() && setB_it != setB.end() && disjoint) {
    if (*setA_it == *setB_it)
      disjoint = false;
    else if (*setA_it < *setB_it)
      setA_it++;
    else
      setB_it++;
  }

  return disjoint;
}

}  // namespace

inline const std::set<oid_t> PlanUtil::GetAffectedIndexes(
    catalog::CatalogCache &catalog_cache, parser::SQLStatement *sql_stmt) {
  std::set<oid_t> index_oids;
  std::string db_name, table_name;
  switch (sql_stmt->GetType()) {
    // For INSERT, DELETE, all indexes are affected
    case StatementType::INSERT: {
      auto insert_stmt = static_cast<parser::InsertStatement *>(sql_stmt);
      db_name = insert_stmt->GetDatabaseName();
      table_name = insert_stmt->GetTableName();
    }
    PELOTON_FALLTHROUGH;
    case StatementType::DELETE: {
      if (table_name.empty() || db_name.empty()) {
        auto delete_stmt = static_cast<parser::DeleteStatement *>(sql_stmt);
        db_name = delete_stmt->GetDatabaseName();
        table_name = delete_stmt->GetTableName();
      }
      auto indexes_map = catalog_cache.GetDatabaseObject(db_name)
                             ->GetTableObject(table_name)
                             ->GetIndexObjects();
      for (auto &index : indexes_map) {
        index_oids.insert(index.first);
      }
    }
    break;
    case StatementType::UPDATE: {
      auto update_stmt = static_cast<parser::UpdateStatement *>(sql_stmt);
      db_name = update_stmt->table->GetDatabaseName();
      table_name = update_stmt->table->GetTableName();
      auto db_object = catalog_cache.GetDatabaseObject(db_name);
      auto table_object = db_object->GetTableObject(table_name);

      auto &update_clauses = update_stmt->updates;
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
        if (!isDisjoint(key_attrs_set, update_oids)) {
          LOG_TRACE("Index (%s) is affected",
                    index.second->GetIndexName().c_str());
          index_oids.insert(index.first);
        }
      }
    } break;
    default:
      LOG_TRACE("Does not support finding affected indexes for query type: %d",
                static_cast<int>(sql_stmt->GetType()));
  }
  return (index_oids);
}

}  // namespace planner
}  // namespace peloton
