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
#include "optimizer/operators.h"
#include "traffic_cop/traffic_cop.h"

namespace peloton {
namespace brain {

unsigned long WhatIfIndex::index_seq_no = 0;

std::unique_ptr<optimizer::OptimizerPlanInfo>
WhatIfIndex::GetCostAndBestPlanTree(std::shared_ptr<parser::SQLStatement> query,
                                    IndexConfiguration &config,
                                    std::string database_name,
                                    concurrency::TransactionContext *txn) {
  LOG_DEBUG("***** GetCostAndBestPlanTree **** \n");
  // Find all the tables that are referenced in the parsed query.
  std::unordered_set<std::string> tables_used;
  GetTablesReferenced(query, tables_used);
  LOG_DEBUG("Tables referenced count: %ld", tables_used.size());
  PELOTON_ASSERT(tables_used.size() > 0);

  // TODO [vamshi]: Improve this loop.
  // Load the indexes into the cache for each table so that the optimizer uses
  // the indexes that we provide.
  for (auto table_name : tables_used) {
    // Load the tables into cache.
    // TODO [vamshi]: If the table is deleted, then this will throw an
    // exception. Handle it.
    auto table_object = catalog::Catalog::GetInstance()->GetTableObject(
        database_name, DEFUALT_SCHEMA_NAME, table_name, txn);

    // Evict all the existing real indexes and
    // insert the what-if indexes into the cache.
    table_object->EvictAllIndexObjects();

    // Upon evict index objects, the index set becomes
    // invalid. Set it to valid so that we don't query
    // the catalog again while doing query optimization later.
    table_object->SetValidIndexObjects(true);

    auto index_set = config.GetIndexes();
    for (auto it = index_set.begin(); it != index_set.end(); it++) {
      auto index = *it;
      if (index->table_oid == table_object->GetTableOid()) {
        auto index_catalog_obj = CreateIndexCatalogObject(index.get());
        table_object->InsertIndexObject(index_catalog_obj);
        LOG_DEBUG("Created a new hypothetical index %d on table: %d",
                  index_catalog_obj->GetIndexOid(),
                  index_catalog_obj->GetTableOid());
        for (auto col : index_catalog_obj->GetKeyAttrs()) {
          (void)col;  // for debug mode.
          LOG_DEBUG("Cols: %d", col);
        }
      }
    }
  }

  // Perform query optimization with the hypothetical indexes
  optimizer::Optimizer optimizer;
  auto opt_info_obj = optimizer.GetOptimizedPlanInfo(query, txn);

  LOG_DEBUG("Query: %s", query->GetInfo().c_str());
  LOG_DEBUG("Hypothetical config: %s", config.ToString().c_str());
  LOG_DEBUG("Got cost %lf", opt_info_obj->cost);
  LOG_DEBUG("Plan type: %s", opt_info_obj->plan->GetInfo().c_str());
  return opt_info_obj;
}

void WhatIfIndex::GetTablesReferenced(
    std::shared_ptr<parser::SQLStatement> query,
    std::unordered_set<std::string> &table_names) {
  // populated if this query has a cross-product table references.
  std::vector<std::unique_ptr<parser::TableRef>> *table_cp_list;

  switch (query->GetType()) {
    case StatementType::INSERT: {
      auto sql_statement = dynamic_cast<parser::InsertStatement *>(query.get());
      table_names.insert(sql_statement->table_ref_->GetTableName());
      break;
    }

    case StatementType::DELETE: {
      auto sql_statement = dynamic_cast<parser::DeleteStatement *>(query.get());
      table_names.insert(sql_statement->table_ref->GetTableName());
      break;
    }

    case StatementType::UPDATE: {
      auto sql_statement = dynamic_cast<parser::UpdateStatement *>(query.get());
      table_names.insert(sql_statement->table->GetTableName());
      break;
    }

    case StatementType::SELECT: {
      auto sql_statement = dynamic_cast<parser::SelectStatement *>(query.get());
      // Select can operate on more than 1 table.
      switch (sql_statement->from_table->type) {
        case TableReferenceType::NAME: {
          // Single table.
          LOG_DEBUG("Table name is %s",
                    sql_statement->from_table.get()->GetTableName().c_str());
          table_names.insert(sql_statement->from_table.get()->GetTableName());
          break;
        }
        case TableReferenceType::JOIN: {
          // Get all table names in the join.
          std::deque<parser::TableRef *> queue;
          queue.push_back(sql_statement->from_table->join->left.get());
          queue.push_back(sql_statement->from_table->join->right.get());
          while (queue.size() != 0) {
            auto front = queue.front();
            queue.pop_front();
            if (front == nullptr) {
              continue;
            }
            if (front->type == TableReferenceType::JOIN) {
              queue.push_back(front->join->left.get());
              queue.push_back(front->join->right.get());
            } else if (front->type == TableReferenceType::NAME) {
              table_names.insert(front->GetTableName());
            } else {
              PELOTON_ASSERT(false);
            }
          }
          break;
        }
        case TableReferenceType::SELECT: {
          GetTablesReferenced(std::shared_ptr<parser::SQLStatement>(
                                  sql_statement->from_table->select),
                              table_names);
          break;
        }
        case TableReferenceType::CROSS_PRODUCT: {
          // Cross product table list.
          table_cp_list = &(sql_statement->from_table->list);
          for (auto &table : *table_cp_list) {
            table_names.insert(table->GetTableName());
          }
          break;
        }
        case TableReferenceType::INVALID: {
          LOG_ERROR("Invalid table reference");
          return;
        }
      }
      break;
    }
    default: {
      LOG_ERROR("Cannot handle DDL statements");
      PELOTON_ASSERT(false);
    }
  }
}

std::shared_ptr<catalog::IndexCatalogObject>
WhatIfIndex::CreateIndexCatalogObject(HypotheticalIndexObject *index_obj) {
  // Create an index name:
  // index_<database_name>_<table_name>_<col_oid1>_<col_oid2>_...
  std::ostringstream index_name_oss;
  index_name_oss << "index_" << index_obj->db_oid << "_"
                 << index_obj->table_oid;
  for (auto it = index_obj->column_oids.begin();
       it != index_obj->column_oids.end(); it++) {
    index_name_oss << (*it) << "_";
  }
  // TODO: For now, we assume BW-TREE and DEFAULT index constraint type for the
  // hypothetical indexes
  // TODO: Support unique keys.
  // Create a dummy catalog object.
  auto col_oids = std::vector<oid_t>(index_obj->column_oids.begin(),
                             index_obj->column_oids.end());
  auto index_cat_obj = std::shared_ptr<catalog::IndexCatalogObject>(
      new catalog::IndexCatalogObject(
          index_seq_no++, index_name_oss.str(), index_obj->table_oid,
          IndexType::BWTREE, IndexConstraintType::DEFAULT, false, col_oids));
  return index_cat_obj;
}

}  // namespace brain
}  // namespace peloton
