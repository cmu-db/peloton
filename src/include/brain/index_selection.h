//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_selection.h
//
// Identification: src/include/brain/index_selection.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "configuration.h"
#include "parser/sql_statement.h"
#include "catalog/index_catalog.h"

namespace peloton {
namespace brain {

using namespace parser;
using namespace catalog;

// Represents a workload
class Workload {
private:
  std::vector<SQLStatement*> sql_queries;
public:
  Workload() {}
  void AddQuery(SQLStatement *query) {
    sql_queries.push_back(query);
  }
  std::vector<SQLStatement*> &GetQueries() {
    return sql_queries;
  }
  size_t Size() {
    return sql_queries.size();
  }
};

//===--------------------------------------------------------------------===//
// IndexSelection
//===--------------------------------------------------------------------===//
class IndexSelection {
 public:
  IndexSelection(std::shared_ptr<Workload> query_set);
  std::unique_ptr<Configuration> GetBestIndexes();
private:
  void Enumerate(Configuration &indexes, Configuration &picked_indexes,
                      Workload &workload);
  void GetAdmissableIndexes(SQLStatement *query,
                            Configuration &indexes);
  // members
  std::shared_ptr<Workload> query_set_;
};

}  // namespace brain
}  // namespace peloton
