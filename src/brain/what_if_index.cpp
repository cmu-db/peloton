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
                                    std::string database_name) {
  // Need transaction for fetching catalog information.
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // Find all the tables that are referenced in the parsed query.
  std::vector<std::string> tables_used;
  GetTablesReferenced(query, tables_used);
  LOG_TRACE("Tables referenced count: %ld", tables_used.size());

  // TODO [vamshi]: Improve this loop.
  // Load the indexes into the cache for each table so that the optimizer uses
  // the indexes that we provide.
  for (auto table_name : tables_used) {
    // Load the tables into cache.
    auto table_object = catalog::Catalog::GetInstance()->GetTableObject(
        database_name, DEFUALT_SCHEMA_NAME, table_name, txn);
    // Evict all the existing real indexes and
    // insert the what-if indexes into the cache.
    table_object->EvictAllIndexObjects();
    auto index_set = config.GetIndexes();
    for (auto it = index_set.begin(); it != index_set.end(); it++) {
      auto index = *it;
      if (index->table_oid == table_object->GetTableOid()) {
        auto index_catalog_obj = CreateIndexCatalogObject(index.get());
        table_object->InsertIndexObject(index_catalog_obj);
        LOG_TRACE("Created a new hypothetical index %d on table: %d",
                  index_catalog_obj->GetIndexOid(),
                  index_catalog_obj->GetTableOid());
        for (auto col : index_catalog_obj->GetKeyAttrs()) {
          (void)col;  // for debug mode.
          LOG_TRACE("Cols: %d", col);
        }
      }
    }
    LOG_TRACE("Index Catalog Objects inserted: %ld",
              table_object->GetIndexObjects().size());
  }

  // Perform query optimization with the hypothetical indexes
  optimizer::Optimizer optimizer;
  auto opt_info_obj = optimizer.GetOptimizedPlanInfo(query, txn);

  LOG_TRACE("Query: %s", query->GetInfo().c_str());
  LOG_TRACE("Hypothetical config: %s", config.ToString().c_str());
  LOG_TRACE("Got cost %lf", opt_info_obj->cost);

  txn_manager.CommitTransaction(txn);
  return opt_info_obj;
}

void WhatIfIndex::GetTablesReferenced(
    std::shared_ptr<parser::SQLStatement> query,
    std::vector<std::string> &table_names) {
  // populated if this query has a cross-product table references.
  std::vector<std::unique_ptr<parser::TableRef>> *table_cp_list;

  switch (query->GetType()) {
    case StatementType::INSERT: {
      auto sql_statement = dynamic_cast<parser::InsertStatement *>(query.get());
      table_names.push_back(sql_statement->table_ref_->GetTableName());
      break;
    }

    case StatementType::DELETE: {
      auto sql_statement = dynamic_cast<parser::DeleteStatement *>(query.get());
      table_names.push_back(sql_statement->table_ref->GetTableName());
      break;
    }

    case StatementType::UPDATE: {
      auto sql_statement = dynamic_cast<parser::UpdateStatement *>(query.get());
      table_names.push_back(sql_statement->table->GetTableName());
      break;
    }

    case StatementType::SELECT: {
      auto sql_statement = dynamic_cast<parser::SelectStatement *>(query.get());
      // Select can operate on more than 1 table.
      switch (sql_statement->from_table->type) {
        case TableReferenceType::NAME: {
          // TODO[Siva]: Confirm this from Vamshi
          LOG_TRACE("Table name is %s",
                    sql_statement->from_table.get()->GetTableName().c_str());
          table_names.push_back(
              sql_statement->from_table.get()->GetTableName());
          break;
        }
        case TableReferenceType::JOIN: {
          table_names.push_back(sql_statement->from_table->join->left.get()
                                    ->GetTableName()
                                    .c_str());
          break;
        }
        case TableReferenceType::SELECT: {
          // TODO[vamshi]: Find out what has to be done here?
          break;
        }
        case TableReferenceType::CROSS_PRODUCT: {
          table_cp_list = &(sql_statement->from_table->list);
          for (auto it = table_cp_list->begin(); it != table_cp_list->end();
               it++) {
            table_names.push_back((*it)->GetTableName().c_str());
          }
        }
        default: {
          LOG_ERROR("Invalid select statement type");
          PELOTON_ASSERT(false);
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
  // Create a dummy catalog object.
  auto index_cat_obj = std::shared_ptr<catalog::IndexCatalogObject>(
      new catalog::IndexCatalogObject(index_seq_no++, index_name_oss.str(),
                                      index_obj->table_oid, IndexType::BWTREE,
                                      IndexConstraintType::DEFAULT, false,
                                      std::vector<oid_t>(index_obj->column_oids.begin(), index_obj->column_oids.end())));
  return index_cat_obj;
}

}  // namespace brain
}  // namespace peloton
