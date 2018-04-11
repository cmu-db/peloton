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
  oid_t db_oid;
  oid_t table_oid;
  std::vector<oid_t> column_oids;
  IndexConstraintType type;
};

// Represents a set of hypothetical indexes - An index configuration.
class IndexConfiguration {
public:
  IndexConfiguration();
  void Add(IndexConfiguration &config);
  void AddIndexObject(std::shared_ptr<IndexObject> index_info);
  size_t GetIndexCount();
  const std::set<std::shared_ptr<IndexObject>> &GetIndexes() const;
  const std::string ToString() const;
  bool operator==(const IndexConfiguration &obj) const;
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
  const std::vector<SQLStatement*> &GetQueries() {
    return sql_queries_;
  }
  size_t Size() {
    return sql_queries_.size();
  }
};

}  // namespace brain
}  // namespace peloton
