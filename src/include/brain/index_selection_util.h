//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// configuration.h
//
// Identification: src/include/brain/configuration.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <set>
#include "catalog/index_catalog.h"
#include "parser/sql_statement.h"

namespace peloton {
namespace brain {

using namespace parser;

// Represents a hypothetical index
class IndexObject {
public:
  oid_t db_;
  oid_t table_;
  std::vector<oid_t> columns_;
};

// Represents a set of hypothetical indexes - An index configuration.
class IndexConfiguration {
public:
  IndexConfiguration();
  void Add(IndexConfiguration &config);
  void AddIndexObject(std::shared_ptr<IndexObject> index_info);
  size_t GetIndexCount();
  std::set<std::shared_ptr<IndexObject>> &GetIndexes();
private:
  // The set of hypothetical indexes in the configuration
  std::set<std::shared_ptr<IndexObject>> indexes_;
};

// Represents a workload of SQL queries
class Workload {
private:
  std::vector<SQLStatement*> sql_queries_;
public:
  Workload() {}
  void AddQuery(SQLStatement *query) {
    sql_queries_.push_back(query);
  }
  std::vector<SQLStatement*> &GetQueries() {
    return sql_queries_;
  }
  size_t Size() {
    return sql_queries_.size();
  }
};

}  // namespace brain
}  // namespace peloton
