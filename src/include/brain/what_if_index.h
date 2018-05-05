//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// what_if_index.h
//
// Identification: src/include/brain/what_if_index.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <queue>
#include <vector>

#include "brain/index_selection_util.h"
#include "catalog/catalog.h"
#include "catalog/column_catalog.h"
#include "catalog/database_catalog.h"
#include "catalog/index_catalog.h"
#include "catalog/table_catalog.h"
#include "common/internal_types.h"
#include "optimizer/optimizer.h"
#include "parser/postgresparser.h"

namespace peloton {
namespace brain {

/**
 * @brief Static class to query what-if cost of an index set.
 */
class WhatIfIndex {
 public:
  /**
   * @brief GetCostAndBestPlanTree
   * Perform optimization on the given parsed & bound SQL statement and
   * return the best physical plan tree and the cost associated with it.
   *
   * @param query - parsed and bound query
   * @param config - a hypothetical index configuration
   * @param database_name - database name string
   * @return physical plan info
   */
  static std::unique_ptr<optimizer::OptimizerPlanInfo> GetCostAndBestPlanTree(
      std::shared_ptr<parser::SQLStatement> query, IndexConfiguration &config,
      std::string database_name);

 private:
  /**
   * @brief GetTablesUsed
   * Given a parsed & bound query, this function updates all the tables
   * referenced.
   *
   * @param query - a parsed and bound SQL statement
   * @param table_names - where the table names will be stored.
   */
  static void GetTablesReferenced(std::shared_ptr<parser::SQLStatement> query,
                                  std::vector<std::string> &table_names);
  /**
   * @brief Creates a hypothetical index catalog object, that would be used
   * to fill the catalog cache.
   *
   * @param obj - Index object
   * @return index catalog object
   */
  static std::shared_ptr<catalog::IndexCatalogObject> CreateIndexCatalogObject(
    HypotheticalIndexObject *obj);
  /**
   * @brief a monotonically increasing sequence number for creating dummy oids
   * for the given hypothetical indexes.
   */
  static unsigned long index_seq_no;
};

}  // namespace brain
}  // namespace peloton
