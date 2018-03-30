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

#include "include/brain/what_if_index.h"
#include "catalog/table_catalog.h"
#include "traffic_cop/traffic_cop.h"
#include "parser/select_statement.h"
#include "parser/delete_statement.h"
#include "parser/insert_statement.h"
#include "parser/update_statement.h"
#include "optimizer/optimizer.h"
#include "concurrency/transaction_manager_factory.h"

namespace peloton {
namespace brain {
  // WhatIfIndex
  // API to query the cost of a given query for the provided hypothetical indexes.
  // @parse_tree_list: output list of SQL trees of the parser.
  // @indexes: set of indexes (can be real/hypothetical)
  // Real indexes are the indexes which are already present.
  WhatIfIndex::WhatIfIndex(
    std::unique_ptr<parser::SQLStatementList> parse_tree_list,
    std::vector<std::shared_ptr<catalog::IndexCatalogObject>> &indexes,
    std::string database_name) {
    parse_tree_list_ = std::move(parse_tree_list);
    index_set_ = indexes;
    database_name_ = database_name;
  }

  // GetCost()
  // Perform the cost computation for the query.
  // This interfaces with the optimizer to get the cost of the query.
  // If the optimizer doesn't choose any of the provided indexes for the query,
  // the cost returned is infinity.
  double WhatIfIndex::GetCost() {
    double query_cost = COST_INVALID;
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();

    // TODO[vamshi]: For now, take only the first parse tree.
    LOG_DEBUG("Total SQL statements here: %ld", parse_tree_list_->GetNumStatements());
    auto statement = parse_tree_list_->GetStatement(0);

    // Only support the DML statements.
    parser::SelectStatement* select_stmt = nullptr;
    parser::UpdateStatement* update_stmt = nullptr;
    parser::DeleteStatement* delete_stmt = nullptr;
    parser::InsertStatement* insert_stmt = nullptr;

    std::vector<std::string> table_names;

    switch (statement->GetType()) {
      case StatementType::INSERT:
        insert_stmt = dynamic_cast<parser::InsertStatement*>(statement);
        table_names.push_back(insert_stmt->table_ref_->GetTableName());
        break;
      case StatementType::DELETE:
        delete_stmt = dynamic_cast<parser::DeleteStatement*>(statement);
        table_names.push_back(delete_stmt->table_ref->GetTableName());
        break;
      case StatementType::UPDATE:
        update_stmt = dynamic_cast<parser::UpdateStatement*>(statement);
        table_names.push_back(update_stmt->table->GetTableName());
        break;
      case StatementType::SELECT:
        select_stmt = dynamic_cast<parser::SelectStatement*>(statement);
        for (auto &table: select_stmt->from_table->list) {
          table_names.push_back(table->GetTableName());
        }
        break;
      default:
        LOG_WARN("Cannot handle DDL statements");
        PL_ASSERT(false);
    }

    // Load the hypothetical indexes into the cache.
    for (auto table_name: table_names) {
      // Load the tables into cache.
      auto table_object = catalog::Catalog::GetInstance()->GetTableObject(
        database_name_, table_name, txn);
      // Evict and insert the provided indexes into the cache.
      table_object->EvictAllIndexObjects();
      for (auto index: index_set_) {
        if (index->GetTableOid() == table_object->GetTableOid()) {
          table_object->InsertIndexObject(index);
        }
      }
    }

    optimizer::Optimizer optimizer;
    // Get the query cost.
    optimizer.GetOptimizedQueryTree(parse_tree_list_, database_name_, txn);

    txn_manager.CommitTransaction(txn);
    return query_cost;
  }
}
}
