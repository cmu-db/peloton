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
#include <set>
namespace peloton {
namespace brain {

// TODO: Remove these
using namespace parser;
using namespace catalog;


struct Comp
{
  Comp(Workload &workload) {this->w = &workload;}
  bool operator()(const IndexConfiguration &s1, const IndexConfiguration &s2)
  {

//     IndexSelection::GetCost(s1, w);
    // TODO Call CostModel::GetCost(s1, w);
    return s1.GetIndexCount() < s2.GetIndexCount();
  }

  Workload *w;
};


//===--------------------------------------------------------------------===//
// IndexSelection
//===--------------------------------------------------------------------===//
class IndexSelection {
 public:
  IndexSelection(Workload &query_set);
  std::unique_ptr<IndexConfiguration> GetBestIndexes();

private:
  // Cost evaluation related
  double GetCost(IndexConfiguration &config, Workload &workload) const;
  double ComputeCost(IndexConfiguration &config, Workload &workload);
  void Enumerate(IndexConfiguration &indexes,
                 IndexConfiguration &picked_indexes,
                      Workload &workload);


  // Configuration Enumeration Method
  IndexConfiguration ExhaustiveEnumeration(IndexConfiguration &indexes, Workload &workload);

  IndexConfiguration GetRemainingIndexes(IndexConfiguration &indexes, IndexConfiguration top_indexes);


    void GreedySearch(IndexConfiguration &indexes,
                             IndexConfiguration &picked_indexes,
                             Workload &workload);

  // Admissible index selection related
  void GetAdmissibleIndexes(SQLStatement *query,
                            IndexConfiguration &indexes);

  void IndexColsParseWhereHelper(const expression::AbstractExpression *where_expr,
                                 IndexConfiguration &config);
  void IndexColsParseGroupByHelper(std::unique_ptr<GroupByDescription> &where_expr,
                                   IndexConfiguration &config);
  void IndexColsParseOrderByHelper(std::unique_ptr<OrderDescription> &order_by,
                                   IndexConfiguration &config);
  std::shared_ptr<IndexObject> AddIndexColumnsHelper(oid_t database,
                                                     oid_t table, std::vector<oid_t> cols);
  void IndexObjectPoolInsertHelper(const expression::TupleValueExpression *tuple_col,
                                   IndexConfiguration &config);
  // members
  Workload query_set_;
  IndexSelectionContext context_;
};

}  // namespace brain
}  // namespace peloton
