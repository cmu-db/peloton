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
#include "catalog/table_catalog.h"
#include "traffic_cop/traffic_cop.h"
#include "parser/select_statement.h"
#include "parser/delete_statement.h"
#include "parser/insert_statement.h"
#include "parser/update_statement.h"
#include "optimizer/optimizer.h"
#include "optimizer/operators.h"
#include "concurrency/transaction_manager_factory.h"
#include "binder/bind_node_visitor.h"

namespace peloton {
namespace brain {
  // WhatIfIndex
  // API to query the cost of a query for the given hypothetical index set.
  WhatIfIndex::WhatIfIndex() {
    LOG_DEBUG("WhatIfIndex Object initialized");
  }

  // GetCost()
  // Perform the cost computation for the query.
  // This interfaces with the optimizer to get the cost of the query.
  // @parse_tree_list: output list of SQL trees of the parser.
  // @indexes: set of indexes (can be real/hypothetical)
  // Real indexes are the indexes which are already present.
  std::unique_ptr<optimizer::OptimizerContextInfo>
    WhatIfIndex::GetCostAndPlanTree(std::unique_ptr<parser::SQLStatementList> parse_tree_list,
                                    std::vector<std::shared_ptr<catalog::IndexCatalogObject>> &index_set,
                                    std::string database_name) {

    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();

    LOG_DEBUG("Total SQL statements here: %ld", parse_tree_list->GetStatements().size());

    auto parsed_statement = parse_tree_list->GetStatements().at(0).get();

    // Run binder
    auto bind_node_visitor =
      std::unique_ptr<binder::BindNodeVisitor>
        (new binder::BindNodeVisitor(txn, database_name));
    bind_node_visitor->BindNameToNode(parsed_statement);

    // Only support the DML statements.
    union {
      parser::SelectStatement *select_stmt;
      parser::UpdateStatement *update_stmt;
      parser::DeleteStatement *delete_stmt;
      parser::InsertStatement *insert_stmt;
    } sql_statement;

    std::vector<std::string> table_names;

    switch (parsed_statement->GetType()) {
      case StatementType::INSERT:
        sql_statement.insert_stmt = dynamic_cast<parser::InsertStatement*>(parsed_statement);
        table_names.push_back(sql_statement.insert_stmt->table_ref_->GetTableName());
        break;
      case StatementType::DELETE:
        sql_statement.delete_stmt = dynamic_cast<parser::DeleteStatement*>(parsed_statement);
        table_names.push_back(sql_statement.delete_stmt->table_ref->GetTableName());
        break;
      case StatementType::UPDATE:
        sql_statement.update_stmt = dynamic_cast<parser::UpdateStatement*>(parsed_statement);
        table_names.push_back(sql_statement.update_stmt->table->GetTableName());
        break;
      case StatementType::SELECT:
        sql_statement.select_stmt = dynamic_cast<parser::SelectStatement*>(parsed_statement);
        // Select can operate on more than 1 table.
        // TODO: Do for all the reference types.
        if (sql_statement.select_stmt->from_table->type == TableReferenceType::NAME) {
          LOG_INFO("Table name is %s", sql_statement.select_stmt->from_table.get()->GetTableName().c_str());
          table_names.push_back(sql_statement.select_stmt->from_table.get()->GetTableName());
        }
        break;
      default:
        LOG_WARN("Cannot handle DDL statements");
        PL_ASSERT(false);
    }

    LOG_INFO("Tables referenced count: %ld", table_names.size());

    // Load the indexes into the cache for each table so that the optimizer uses
    // the indexes that we provide.
    for (auto table_name: table_names) {
      // Load the tables into cache.
      auto table_object = catalog::Catalog::GetInstance()->GetTableObject(
        database_name, table_name, txn);
      // Evict all the existing real indexes and
      // insert the what-if indexes into the cache.
      table_object->EvictAllIndexObjects();
      for (auto index: index_set) {
        if (index->GetTableOid() == table_object->GetTableOid()) {
          table_object->InsertIndexObject(index);
          LOG_INFO("Created a new hypothetical index %d on table: %d",
                   index->GetIndexOid(),
                   index->GetTableOid());
        }
      }
    }

    // Perform query optimization with the hypothetical indexes
    optimizer::Optimizer optimizer;
    auto opt_info_obj = optimizer.PerformOptimization(parsed_statement, txn);

    txn_manager.CommitTransaction(txn);

    return opt_info_obj;
  }

//  // Search the optimized query plan tree to find all the indexes
//  // that are present.
//  void WhatIfIndex::FindIndexesUsed(optimizer::GroupID root_id,
//                       optimizer::QueryInfo &query_info,
//                       optimizer::OptimizerMetadata &md) {
//    auto group = md.memo.GetGroupByID(root_id);
//    auto expr = group->GetBestExpression(query_info.physical_props);
//
//    if (expr->Op().GetType() == optimizer::OpType::IndexScan && expr->Op().IsPhysical()) {
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
}
}
