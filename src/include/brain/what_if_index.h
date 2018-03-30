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
#include "catalog/catalog.h"
#include "catalog/database_catalog.h"
#include "catalog/table_catalog.h"
#include "catalog/index_catalog.h"
#include "catalog/column_catalog.h"
#include "parser/postgresparser.h"

namespace parser {
  class SQLStatementList;
}

namespace catalog {
  class IndexCatalogObject;
}

namespace peloton {
namespace brain {
#define COST_INVALID -1
  class WhatIfIndex {
  public:
    WhatIfIndex(std::shared_ptr<parser::SQLStatementList> parse_tree_list,
                std::vector<std::shared_ptr<catalog::IndexCatalogObject>> &index_set,
                std::string database_name);

    double GetCost();

  private:
    std::shared_ptr<parser::SQLStatementList> parse_tree_list_;
    std::vector<std::shared_ptr<catalog::IndexCatalogObject>> index_set_;
    std::string database_name_;
  };

}}
