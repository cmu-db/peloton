//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// what_if_index.cpp
//
// Identification: src/brain/what_if_index.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/what_if_index.h"
#include "binder/bind_node_visitor.h"
#include "catalog/table_catalog.h"
#include "concurrency/transaction_manager_factory.h"
#include "optimizer/operators.h"
#include "optimizer/optimizer.h"
#include "parser/delete_statement.h"
#include "parser/insert_statement.h"
#include "parser/select_statement.h"
#include "parser/table_ref.h"
#include "parser/update_statement.h"
#include "traffic_cop/traffic_cop.h"

namespace peloton {
namespace brain {
// GetCostAndPlanTree()
// Perform the cost computation for the query.
// This interfaces with the optimizer to get the cost & physical plan of the
// query.
// @parsed_sql_query: SQL statement
// @index_set: set of indexes to be examined
std::unique_ptr<optimizer::OptimizerPlanInfo> WhatIfIndex::GetCostAndPlanTree(
    parser::SQLStatement *parsed_sql_query,
    std::vector<std::shared_ptr<catalog::IndexCatalogObject>> &index_set,
    std::string database_name) {
  // Need transaction for fetching catalog information.
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // Run binder
  auto bind_node_visitor = std::unique_ptr<binder::BindNodeVisitor>(
      new binder::BindNodeVisitor(txn, database_name));
  bind_node_visitor->BindNameToNode(parsed_sql_query);

  // Find all the tables that are referenced in the parsed query.
  std::vector<std::string> tables_used;
  GetTablesUsed(parsed_sql_query, tables_used);
  LOG_DEBUG("Tables referenced count: %ld", tables_used.size());

  // TODO [vamshi]: Improve this loop.
  // Load the indexes into the cache for each table so that the optimizer uses
  // the indexes that we provide.
  for (auto table_name : tables_used) {
    // Load the tables into cache.
    auto table_object = catalog::Catalog::GetInstance()->GetTableObject(
        database_name, table_name, txn);
    // Evict all the existing real indexes and
    // insert the what-if indexes into the cache.
    table_object->EvictAllIndexObjects();
    for (auto index : index_set) {
      if (index->GetTableOid() == table_object->GetTableOid()) {
        table_object->InsertIndexObject(index);
        LOG_DEBUG("Created a new hypothetical index %d on table: %d",
                  index->GetIndexOid(), index->GetTableOid());
      }
    }
  }

  // Perform query optimization with the hypothetical indexes
  optimizer::Optimizer optimizer;
  auto opt_info_obj = optimizer.PerformOptimization(parsed_sql_query, txn);

  txn_manager.CommitTransaction(txn);

  return opt_info_obj;
}

void WhatIfIndex::GetTablesUsed(parser::SQLStatement *parsed_statement,
                                std::vector<std::string> &table_names) {
  // Only support the DML statements.
  union {
    parser::SelectStatement *select_stmt;
    parser::UpdateStatement *update_stmt;
    parser::DeleteStatement *delete_stmt;
    parser::InsertStatement *insert_stmt;
  } sql_statement;

  // populated if this query has a cross-product table references.
  std::vector<std::unique_ptr<parser::TableRef>> *table_cp_list;

  switch (parsed_statement->GetType()) {
    case StatementType::INSERT:
      sql_statement.insert_stmt =
          dynamic_cast<parser::InsertStatement *>(parsed_statement);
      table_names.push_back(
          sql_statement.insert_stmt->table_ref_->GetTableName());
      break;

    case StatementType::DELETE:
      sql_statement.delete_stmt =
          dynamic_cast<parser::DeleteStatement *>(parsed_statement);
      table_names.push_back(
          sql_statement.delete_stmt->table_ref->GetTableName());
      break;

    case StatementType::UPDATE:
      sql_statement.update_stmt =
          dynamic_cast<parser::UpdateStatement *>(parsed_statement);
      table_names.push_back(sql_statement.update_stmt->table->GetTableName());
      break;

    case StatementType::SELECT:
      sql_statement.select_stmt =
          dynamic_cast<parser::SelectStatement *>(parsed_statement);
      // Select can operate on more than 1 table.
      switch (sql_statement.select_stmt->from_table->type) {
        case TableReferenceType::NAME:
          LOG_DEBUG("Table name is %s",
                    sql_statement.select_stmt->from_table.get()
                        ->GetTableName()
                        .c_str());
          table_names.push_back(
              sql_statement.select_stmt->from_table.get()->GetTableName());
          break;
        case TableReferenceType::JOIN:
          table_names.push_back(
              sql_statement.select_stmt->from_table->join->left.get()
                  ->GetTableName()
                  .c_str());
          break;
        case TableReferenceType::SELECT:
          // TODO[vamshi]: Find out what has to be done here?
          break;
        case TableReferenceType::CROSS_PRODUCT:
          table_cp_list = &(sql_statement.select_stmt->from_table->list);
          for (auto it = table_cp_list->begin(); it != table_cp_list->end();
               it++) {
            table_names.push_back((*it)->GetTableName().c_str());
          }
        default:
          LOG_ERROR("Invalid select statement type");
          PL_ASSERT(false);
      }
      break;

    default:
      LOG_WARN("Cannot handle DDL statements");
      PL_ASSERT(false);
  }
}

//  // Search the optimized query plan tree to find all the indexes
//  // that are present.
//  void WhatIfIndex::FindIndexesUsed(optimizer::GroupID root_id,
//                       optimizer::QueryInfo &query_info,
//                       optimizer::OptimizerMetadata &md) {
//    auto group = md.memo.GetGroupByID(root_id);
//    auto expr = group->GetBestExpression(query_info.physical_props);
//
//    if (expr->Op().GetType() == optimizer::OpType::IndexScan &&
//    expr->Op().IsPhysical()) {
//      auto index = expr->Op().As<optimizer::PhysicalIndexScan>();
//      for (auto hy_index: index_set) {
//        if (index->index_id == hy_index->GetIndexOid()) {
//          indexes_used.push_back(hy_index);
//        }
//      }
//    }
//
//    // Explore children.
//    auto child_gids = expr->GetChildGroupIDs();
//    for (auto child: child_gids) {
//      FindIndexesUsed(child, query_info, md);
//    }
//  }
}  // namespace brain
}  // namespace peloton
