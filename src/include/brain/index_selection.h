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

#include "brain/index_selection_context.h"
#include "expression/tuple_value_expression.h"
#include "brain/index_selection_util.h"
#include "catalog/index_catalog.h"
#include "parser/sql_statement.h"

namespace peloton {
namespace brain {

//===--------------------------------------------------------------------===//
// IndexSelection
//===--------------------------------------------------------------------===//
class IndexSelection {
 public:
  IndexSelection(Workload &query_set);
  void GetBestIndexes(IndexConfiguration &final_indexes);
  void GetAdmissibleIndexes(SQLStatement *query,
                            IndexConfiguration &indexes);
private:
  void GenCandidateIndexes(IndexConfiguration &config, IndexConfiguration &admissible_config,
                           Workload &workload);
  // Cost evaluation related
  double GetCost(IndexConfiguration &config, Workload &workload) const;
  double ComputeCost(IndexConfiguration &config, Workload &workload);
  void Enumerate(IndexConfiguration &indexes,
                 IndexConfiguration &picked_indexes,
                      Workload &workload);
  // Admissible index selection related
  void IndexColsParseWhereHelper(const expression::AbstractExpression *where_expr,
                                 IndexConfiguration &config);
  void IndexColsParseGroupByHelper(std::unique_ptr<GroupByDescription> &where_expr,
                                   IndexConfiguration &config);
  void IndexColsParseOrderByHelper(std::unique_ptr<OrderDescription> &order_by,
                                   IndexConfiguration &config);
  std::shared_ptr<IndexObject> AddIndexColumnsHelper(oid_t database,
                                                     oid_t table, std::vector<oid_t> cols);
  IndexConfiguration GenMultiColumnIndexes(IndexConfiguration &config, IndexConfiguration &single_column_indexes);
  void IndexObjectPoolInsertHelper(const expression::TupleValueExpression *tuple_col,
                                   IndexConfiguration &config);
  IndexConfiguration CrossProduct(const IndexConfiguration &config,
      const IndexConfiguration &single_column_indexes);
  // members
  Workload query_set_;
  IndexSelectionContext context_;
};

}  // namespace brain
}  // namespace peloton
