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
#include "brain/index_selection_util.h"

namespace peloton {
namespace brain {

unsigned long WhatIfIndex::index_seq_no = 0;

std::unique_ptr<optimizer::OptimizerPlanInfo>
WhatIfIndex::GetCostAndBestPlanTree(std::shared_ptr<parser::SQLStatement> query,
                                    IndexConfiguration &config,
                                    std::string database_name,
                                    concurrency::TransactionContext *txn) {
  // Find all the tables that are referenced in the parsed query.
  std::unordered_set<std::string> tables_used;
  Workload::GetTableNamesReferenced(query, tables_used);
  return GetCostAndBestPlanTree(std::make_pair(query, tables_used),
                                config, database_name, txn);
}

std::unique_ptr<optimizer::OptimizerPlanInfo>
WhatIfIndex::GetCostAndBestPlanTree(
    std::pair<std::shared_ptr<parser::SQLStatement>,
              std::unordered_set<std::string>> query,
    IndexConfiguration &config, std::string database_name,
    concurrency::TransactionContext *txn) {
  LOG_DEBUG("***** GetCostAndBestPlanTree **** \n");

  LOG_DEBUG("Tables referenced count: %ld", tables_used.size());
  PELOTON_ASSERT(tables_used.size() > 0);

  // TODO [vamshi]: Improve this loop.
  // Load the indexes into the cache for each table so that the optimizer uses
  // the indexes that we provide.
  for (auto table_name : query.second) {
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
  auto opt_info_obj = optimizer.GetOptimizedPlanInfo(query.first, txn);

  LOG_DEBUG("Query: %s", query->GetInfo().c_str());
  LOG_DEBUG("Hypothetical config: %s", config.ToString().c_str());
  LOG_DEBUG("Got cost %lf", opt_info_obj->cost);
  LOG_DEBUG("Plan type: %s", opt_info_obj->plan->GetInfo().c_str());
  return opt_info_obj;
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
