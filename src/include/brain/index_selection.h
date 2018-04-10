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

#include "brain/index_configuration.h"
#include "catalog/index_catalog.h"
#include "parser/sql_statement.h"

namespace peloton {
namespace brain {

// Represents a workload
class Workload {
 private:
  std::vector<parser::SQLStatement *> sql_queries;

 public:
  Workload() {}
  void AddQuery(parser::SQLStatement *query) { sql_queries.push_back(query); }
  std::vector<parser::SQLStatement *> &GetQueries() { return sql_queries; }
  size_t Size() { return sql_queries.size(); }
};

//===--------------------------------------------------------------------===//
// IndexSelection
//===--------------------------------------------------------------------===//
class IndexSelection {
 public:
  IndexSelection(std::shared_ptr<Workload> query_set);
  std::unique_ptr<IndexConfiguration> GetBestIndexes();

 private:
  void Enumerate(IndexConfiguration &indexes,
                 IndexConfiguration &picked_indexes, Workload &workload);
  void GetAdmissibleIndexes(parser::SQLStatement *query,
                            IndexConfiguration &indexes);
  void IndexColsParseWhereHelper(
      std::unique_ptr<expression::AbstractExpression> &where_expr,
      IndexConfiguration &config);
  void IndexColsParseGroupByHelper(
      std::unique_ptr<parser::GroupByDescription> &where_expr,
      IndexConfiguration &config);
  void IndexColsParseOrderByHelper(
      std::unique_ptr<parser::OrderDescription> &order_by,
      IndexConfiguration &config);
  // members
  std::shared_ptr<Workload> query_set_;
};

}  // namespace brain
}  // namespace peloton
