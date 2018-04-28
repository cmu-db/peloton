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

              table_id =
                  db_object->GetTableObject(tuple_value_expr->GetTableName())
                      ->GetTableOid();
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

      // TODO: should transaction commit or not?
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
