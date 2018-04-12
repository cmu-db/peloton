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

namespace parser {
class SQLStatementList;
}

namespace catalog {
class IndexCatalogObject;
}

namespace optimizer {
class QueryInfo;
class OptimizerContextInfo;
}  // namespace optimizer

namespace peloton {
namespace brain {

// Static class to query what-if cost of an index set.
class WhatIfIndex {
 public:
  static std::unique_ptr<optimizer::OptimizerPlanInfo> GetCostAndPlanTree(
      parser::SQLStatement *parsed_query, IndexConfiguration &config,
      std::string database_name);

 private:
  static void FindIndexesUsed(optimizer::GroupID root_id,
                              optimizer::QueryInfo &query_info,
                              optimizer::OptimizerMetadata &md);
  static void GetTablesUsed(parser::SQLStatement *statement,
                            std::vector<std::string> &table_names);
  static std::shared_ptr<catalog::IndexCatalogObject> CreateIndexCatalogObject(
      IndexObject *obj);
  static unsigned long index_seq_no;
};

}  // namespace brain
}  // namespace peloton
