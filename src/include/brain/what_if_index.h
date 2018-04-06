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

#include "catalog/catalog.h"
#include "catalog/database_catalog.h"
#include "catalog/table_catalog.h"
#include "catalog/index_catalog.h"
#include "catalog/column_catalog.h"
#include "parser/postgresparser.h"
#include "common/internal_types.h"
#include "optimizer/optimizer.h"

namespace parser {
  class SQLStatementList;
}

namespace catalog {
  class IndexCatalogObject;
}

namespace optimizer {
  class QueryInfo;
  class OptimizerContextInfo;
}

namespace peloton {
namespace brain {
#define COST_INVALID -1
  class WhatIfIndex {
  public:
    WhatIfIndex();
    std::unique_ptr<optimizer::OptimizerContextInfo>
      GetCostAndPlanTree(std::unique_ptr<parser::SQLStatementList> &parse_tree_list,
                            std::vector<std::shared_ptr<catalog::IndexCatalogObject>> &indexes,
                            std::string database_name);

  private:

    void FindIndexesUsed(optimizer::GroupID root_id,
                         optimizer::QueryInfo &query_info,
                         optimizer::OptimizerMetadata &md);
  };

}}
