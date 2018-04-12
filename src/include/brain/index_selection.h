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

#include <set>

#include "brain/index_selection_context.h"
#include "brain/index_selection_util.h"
#include "catalog/index_catalog.h"
#include "expression/tuple_value_expression.h"
#include "parser/sql_statement.h"

namespace peloton {
namespace brain {

struct Comp {
  Comp(Workload &workload) { this->w = &workload; }
  bool operator()(const IndexConfiguration &s1, const IndexConfiguration &s2) {
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
  IndexSelection(Workload &query_set, size_t max_index_cols,
                 size_t enum_threshold, size_t num_indexes);
  void GetBestIndexes(IndexConfiguration &final_indexes);
  void GetAdmissibleIndexes(SQLStatement *query, IndexConfiguration &indexes);

  /**
   * @brief GenerateCandidateIndexes.
   * If the admissible config set is empty, generate
   * the single-column (admissible) indexes for each query from the provided queries
   * and prune the useless ones. This becomes candidate index set. If not empty, prune
   * the useless indexes from the candidate set for the given workload.
   *
   * @param candidate_config - new candidate index to be pruned.
   * @param admissible_config - admissible index set of the queries
   * @param workload - queries
   */
  void GenerateCandidateIndexes(IndexConfiguration &candidate_config,
                           IndexConfiguration &admissible_config,
                           Workload &workload);
  void Enumerate(IndexConfiguration &indexes, IndexConfiguration &top_indexes, Workload &workload, size_t k);
  void GenerateMultiColumnIndexes(IndexConfiguration &config,
                             IndexConfiguration &single_column_indexes,
                             IndexConfiguration &result);

private:

  /**
   * @brief PruneUselessIndexes
   * Delete the indexes from the configuration which do not help at least one of the
   * queries in the workload
   *
   * @param config - index set
   * @param workload - queries
   */
  void PruneUselessIndexes(IndexConfiguration &config, Workload &workload);
  double GetCost(IndexConfiguration &config, Workload &workload) const;
  double ComputeCost(IndexConfiguration &config, Workload &workload);

  // Configuration Enumeration related
  void ExhaustiveEnumeration(IndexConfiguration &indexes, IndexConfiguration &top_indexes, Workload &workload);
  void GreedySearch(IndexConfiguration &indexes,
                    IndexConfiguration &picked_indexes,
                    Workload &workload, size_t k);

  // Admissible index selection related
  void IndexColsParseWhereHelper(
      const expression::AbstractExpression *where_expr,
      IndexConfiguration &config);
  void IndexColsParseGroupByHelper(
      std::unique_ptr<GroupByDescription> &where_expr,
      IndexConfiguration &config);
  void IndexColsParseOrderByHelper(std::unique_ptr<OrderDescription> &order_by,
                                   IndexConfiguration &config);
  std::shared_ptr<IndexObject> AddIndexColumnsHelper(oid_t database,
                                                     oid_t table,
                                                     std::vector<oid_t> cols);
  void IndexObjectPoolInsertHelper(
      const expression::TupleValueExpression *tuple_col,
      IndexConfiguration &config);
  void CrossProduct(
      const IndexConfiguration &config,
      const IndexConfiguration &single_column_indexes,
      IndexConfiguration &result);

  // members
  Workload query_set_;
  IndexSelectionContext context_;
};

}  // namespace brain
}  // namespace peloton
