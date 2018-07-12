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

#include "planner/plan_util.h"
#include <set>
#include <string>
#include "catalog/catalog_cache.h"
#include "catalog/column_catalog.h"
#include "catalog/constraint_catalog.h"
#include "catalog/database_catalog.h"
#include "catalog/index_catalog.h"
#include "catalog/table_catalog.h"
#include "common/statement.h"
#include "concurrency/transaction_manager_factory.h"
#include "expression/abstract_expression.h"
#include "expression/expression_util.h"
#include "optimizer/abstract_optimizer.h"
#include "optimizer/optimizer.h"
#include "parser/delete_statement.h"
#include "parser/insert_statement.h"
#include "parser/sql_statement.h"
#include "parser/update_statement.h"
#include "util/set_util.h"

namespace peloton {
namespace planner {

const std::set<oid_t> PlanUtil::GetPrimaryKeyColumns(
    std::unordered_map<oid_t, std::shared_ptr<catalog::ConstraintCatalogEntry>>
    table_constraint_entries) {
  std::set<oid_t> pkey_set;
  for(const auto &kv: table_constraint_entries) {
    if(kv.second->GetConstraintType() == ConstraintType::PRIMARY) {
      for(const auto& col_oid: kv.second->GetColumnIds()) {
        pkey_set.insert(col_oid);
      }
    }
  }
  return pkey_set;
}

const std::vector<col_triplet> PlanUtil::GetAffectedIndexes(
    catalog::CatalogCache &catalog_cache, const parser::SQLStatement &sql_stmt,
    const bool ignore_primary) {
  std::vector<col_triplet> index_triplets;
  std::string db_name, table_name, schema_name;
  std::shared_ptr<catalog::DatabaseCatalogEntry> db_object;
  std::shared_ptr<catalog::TableCatalogEntry> table_object;
  oid_t db_oid, table_oid;
  switch (sql_stmt.GetType()) {
    // For INSERT, DELETE, all indexes are affected
    case StatementType::INSERT: {
      auto &insert_stmt =
          static_cast<const parser::InsertStatement &>(sql_stmt);
      db_name = insert_stmt.GetDatabaseName();
      table_name = insert_stmt.GetTableName();
      db_object = catalog_cache.GetDatabaseObject(db_name);
      db_oid = db_object->GetDatabaseOid();
      schema_name = insert_stmt.GetSchemaName();
      table_object = db_object->GetTableCatalogEntry(table_name, schema_name);
      table_oid = table_object->GetTableOid();
    }
      PELOTON_FALLTHROUGH;
    case StatementType::DELETE: {
      if (table_name.empty() || db_name.empty() || schema_name.empty()) {
        auto &delete_stmt =
            static_cast<const parser::DeleteStatement &>(sql_stmt);
        db_name = delete_stmt.GetDatabaseName();
        table_name = delete_stmt.GetTableName();
        db_object = catalog_cache.GetDatabaseObject(db_name);
        db_oid = db_object->GetDatabaseOid();
        schema_name = delete_stmt.GetSchemaName();
        table_object = db_object->GetTableCatalogEntry(table_name, schema_name);
        table_oid = table_object->GetTableOid();
      }
      auto indexes_map = catalog_cache.GetDatabaseObject(db_name)
          ->GetTableCatalogEntry(table_name, schema_name)
          ->GetIndexCatalogEntries();
      for (auto &index : indexes_map) {
        bool add_index = true;

        if (ignore_primary) {
          auto pkey_cols_table = GetPrimaryKeyColumns(table_object->GetConstraintCatalogEntries());
          const auto col_oids = index.second->GetKeyAttrs();
          for (const auto col_oid : col_oids) {
            if(pkey_cols_table.find(col_oid) != pkey_cols_table.end()) {
              add_index = false;
              break;
            }
          }
        }

        if (add_index) {
          index_triplets.emplace_back(db_oid, table_oid, index.first);
        }
      }

    } break;
    case StatementType::UPDATE: {
      auto &update_stmt =
          static_cast<const parser::UpdateStatement &>(sql_stmt);
      db_name = update_stmt.table->GetDatabaseName();
      table_name = update_stmt.table->GetTableName();
      db_object = catalog_cache.GetDatabaseObject(db_name);
      db_oid = db_object->GetDatabaseOid();
      schema_name = update_stmt.table->GetSchemaName();
      auto table_object = catalog_cache.GetDatabaseObject(db_name)
          ->GetTableCatalogEntry(table_name, schema_name);
      table_oid = table_object->GetTableOid();

      auto &update_clauses = update_stmt.updates;
      std::set<oid_t> update_oids;
      for (const auto &update_clause : update_clauses) {
        LOG_TRACE("Affected column name for table(%s) in UPDATE query: %s",
                  table_name.c_str(), update_clause->column.c_str());
        auto col_object =
            table_object->GetColumnCatalogEntry(update_clause->column);
        update_oids.insert(col_object->GetColumnId());
      }

      auto indexes_map = table_object->GetIndexCatalogEntries();
      for (auto &index : indexes_map) {
        LOG_TRACE("Checking if UPDATE query affects index: %s",
                  index.second->GetIndexName().c_str());
        const std::vector<oid_t> &key_attrs =
            index.second->GetKeyAttrs();  // why it's a vector, and not set?
        const std::set<oid_t> key_attrs_set(key_attrs.begin(), key_attrs.end());
        if (!SetUtil::IsDisjoint(key_attrs_set, update_oids)) {
          LOG_TRACE("Index (%s) is affected",
                    index.second->GetIndexName().c_str());
          bool add_index = true;

          if (ignore_primary) {
            auto pkey_cols_table = GetPrimaryKeyColumns(table_object->GetConstraintCatalogEntries());
            const auto col_oids = index.second->GetKeyAttrs();
            for (const auto col_oid : col_oids) {
              if(pkey_cols_table.find(col_oid) != pkey_cols_table.end()) {
                add_index = false;
                break;
              }
            }
          }

          if (add_index) {
            index_triplets.emplace_back(db_oid, table_oid, index.first);
          }
        }
      }
    } break;
    case StatementType::SELECT:
      break;
    default:
      LOG_TRACE("Does not support finding affected indexes for query type: %d",
                static_cast<int>(sql_stmt.GetType()));
  }
  return (index_triplets);
}

const std::vector<col_triplet> PlanUtil::GetIndexableColumns(
    catalog::CatalogCache &catalog_cache,
    std::unique_ptr<parser::SQLStatementList> sql_stmt_list,
    const std::string &db_name) {
  std::vector<col_triplet> column_oids;
  std::string table_name;
  oid_t database_id, table_id;

  // Assume that there is only one SQLStatement in the list
  auto sql_stmt = sql_stmt_list->GetStatement(0);
  switch (sql_stmt->GetType()) {
    // 1) use optimizer to get the plan tree
    // 2) aggregate results from all the leaf scan nodes
    case StatementType::UPDATE:
      PELOTON_FALLTHROUGH;
    case StatementType::DELETE:
      PELOTON_FALLTHROUGH;
    case StatementType::SELECT: {
      std::unique_ptr<optimizer::AbstractOptimizer> optimizer =
          std::unique_ptr<optimizer::AbstractOptimizer>(
              new optimizer::Optimizer());

      auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
      auto txn = txn_manager.BeginTransaction();

      try {
        auto plan = optimizer->BuildPelotonPlanTree(sql_stmt_list, txn);

        auto db_object = catalog_cache.GetDatabaseObject(db_name);
        database_id = db_object->GetDatabaseOid();

        // Perform a breadth first search on plan tree
        std::queue<const AbstractPlan *> scan_queue;
        const AbstractPlan *temp_ptr;
        scan_queue.emplace(plan.get());

        while (!scan_queue.empty()) {
          temp_ptr = scan_queue.front();
          scan_queue.pop();

          // Leaf scanning node
          if (PlanNodeType::SEQSCAN == temp_ptr->GetPlanNodeType() ||
              PlanNodeType::INDEXSCAN == temp_ptr->GetPlanNodeType()) {
            auto temp_scan_ptr = static_cast<const AbstractScan *>(temp_ptr);

            table_id = temp_scan_ptr->GetTable()->GetOid();

            // Aggregate columns scanned in predicates
            ExprSet expr_set;
            auto predicate_ptr = temp_scan_ptr->GetPredicate();
            std::unique_ptr<expression::AbstractExpression> copied_predicate;
            if (nullptr == predicate_ptr) {
              copied_predicate = nullptr;
            } else {
              copied_predicate =
                  std::unique_ptr<expression::AbstractExpression>(
                      predicate_ptr->Copy());
            }
            expression::ExpressionUtil::GetTupleValueExprs(
                expr_set, copied_predicate.get());

            for (const auto expr : expr_set) {
              auto tuple_value_expr =
                  static_cast<const expression::TupleValueExpression *>(expr);
              column_oids.emplace_back(database_id, table_id,
                                       (oid_t)tuple_value_expr->GetColumnId());
            }

          } else {
            for (uint32_t idx = 0; idx < temp_ptr->GetChildrenSize(); ++idx) {
              scan_queue.emplace(temp_ptr->GetChild(idx));
            }
          }
        }

      } catch (Exception &e) {
        LOG_ERROR("Error in BuildPelotonPlanTree: %s", e.what());
      }

      txn_manager.AbortTransaction(txn);
    } break;
    default:
      LOG_TRACE("Return nothing for query type: %d",
                static_cast<int>(sql_stmt->GetType()));
  }
  return (column_oids);
}

}  // namespace planner
}  // namespace peloton
