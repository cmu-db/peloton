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
#include "brain/index_selection_util.h"
#include "catalog/index_catalog.h"
#include "expression/tuple_value_expression.h"
#include "parser/sql_statement.h"

namespace peloton {
namespace brain {

/**
 * @brief Comparator for set of (Index Configuration, Cost)
 */
struct IndexConfigComparator {
  IndexConfigComparator(Workload &workload) { this->w = &workload; }
  bool operator()(const std::pair<IndexConfiguration, double> &s1,
                  const std::pair<IndexConfiguration, double> &s2) const {
    // Order by cost. If cost is same, then by the number of indexes
    // Unless the configuration is exactly the same, get some ordering

    if (s1.second < s2.second) {
      return true;
    } else if (s1.second > s2.second) {
      return false;
    } else {
      if (s1.first.GetIndexCount() > s2.first.GetIndexCount()) {
        return true;
      } else if (s1.first.GetIndexCount() < s2.first.GetIndexCount()) {
        return false;
      } else {
        // TODO[Siva]: Change this to a better one, choose the one with bigger/
        // smaller indexes
        return (s1.first.ToString() < s2.first.ToString());
      }
    }
  }

  Workload *w;
};

//===--------------------------------------------------------------------===//
// IndexSelection
//===--------------------------------------------------------------------===//

class IndexSelection {
 public:
  /**
   * IndexSelection
   *
   * @param query_set set of queries as a workload
   * @param knobs the tunable parameters of the algorithm that includes
   * number of indexes to be chosen, threshold for naive enumeration,
   * maximum number of columns in each index.
   */
  IndexSelection(Workload &query_set, IndexSelectionKnobs knobs,
                 concurrency::TransactionContext *txn);

  /**
   * @brief The main external API for the Index Prediction Tool
   * @returns The best possible Index Congurations for the workload
   */
  void GetBestIndexes(IndexConfiguration &final_indexes);

  /**
   * @brief Gets the indexable columns of a given query
   */
  void GetAdmissibleIndexes(std::shared_ptr<parser::SQLStatement> query,
                            IndexConfiguration &indexes);

  /**
   * @brief GenerateCandidateIndexes.
   * If the admissible config set is empty, generate
   * the single-column (admissible) indexes for each query from the provided
   * queries and prune the useless ones. This becomes candidate index set. If
   * not empty, prune the useless indexes from the candidate set for the given
   * workload.
   *
   * @param candidate_config - new candidate index to be pruned.
   * @param admissible_config - admissible index set of the queries
   * @param workload - queries
   */
  void GenerateCandidateIndexes(IndexConfiguration &candidate_config,
                                IndexConfiguration &admissible_config,
                                Workload &workload);

  /**
   * @brief gets the top k indexes for the workload which would reduce the cost
   * of executing them
   *
   * @param indexes - the indexes in the workload
   * @param top_indexes - the top k cheapest indexes in the workload are
   * returned through this parameter
   * @param workload - the given workload
   * @param k - the number of indexes to return
   */
  void Enumerate(IndexConfiguration &indexes, IndexConfiguration &top_indexes,
                 Workload &workload, size_t k);

  /**
   * @brief generate multi-column indexes from the single column indexes by
   * doing a cross product and adds it into the result.
   *
   * @param config - the set of candidate indexes chosen after the enumeration
   * @param single_column_indexes - the set of admissible single column indexes
   * @param result - return the set of multi column indexes
   */
  void GenerateMultiColumnIndexes(IndexConfiguration &config,
                                  IndexConfiguration &single_column_indexes,
                                  IndexConfiguration &result);

  /**
   * @brief Add a given configuration to the IndexObject pool
   * return the corresponding shared pointer if the object already exists in
   * the pool. Otherwise create one and return.
   * Currently, this is used only for unit testing
   */
  std::shared_ptr<HypotheticalIndexObject> AddConfigurationToPool(
      HypotheticalIndexObject object);

 private:
  /**
   * @brief PruneUselessIndexes
   * Delete the indexes from the configuration which do not help at least one of
   * the queries in the workload
   *
   * @param config - index set
   * @param workload - queries
   * @param pruned_config - result configuration
   */
  void PruneUselessIndexes(IndexConfiguration &config, Workload &workload,
                           IndexConfiguration &pruned_config);

  /**
   * @brief Gets the cost of an index configuration for a given workload. It
   * would call the What-If API appropriately and stores the results in the memo
   * table
   */
  double ComputeCost(IndexConfiguration &config, Workload &workload);

  // Configuration Enumeration related
  /**
   * @brief Gets the cheapest indexes through naive exhaustive enumeration by
   * generating all possible subsets of size <= m where m is a tunable parameter
   */
  void ExhaustiveEnumeration(IndexConfiguration &indexes,
                             IndexConfiguration &top_indexes,
                             Workload &workload);

  /**
   * @brief Gets the remaining cheapest indexes through greedy search
   */
  void GreedySearch(IndexConfiguration &indexes,
                    IndexConfiguration &remaining_indexes, Workload &workload,
                    size_t num_indexes);

  // Admissible index selection related
  /**
   * @brief Helper to parse the order where in the SQL statements such as
   * select, delete, update.
   */
  void IndexColsParseWhereHelper(
      const expression::AbstractExpression *where_expr,
      IndexConfiguration &config);

  /**
   * @brief Helper to parse the group by clause in the SQL statements such as
   * select, delete, update.
   */
  void IndexColsParseGroupByHelper(
      std::unique_ptr<parser::GroupByDescription> &where_expr,
      IndexConfiguration &config);

  /**
   * @brief Helper to parse the order by clause in the SQL statements such as
   * select, delete, update.
   */
  void IndexColsParseOrderByHelper(
      std::unique_ptr<parser::OrderDescription> &order_by,
      IndexConfiguration &config);

  /**
   * @brief Helper function to convert a tuple of <db_oid, table_oid, col_oid>
   * to an IndexObject and store into the IndexObject shared pool.
   *
   * @param - tuple_col: representation of a column
   * @param - config: returns a new index object here
   */
  void IndexObjectPoolInsertHelper(
      const std::tuple<oid_t, oid_t, oid_t> &tuple_col,
      IndexConfiguration &config);

  /**
   * @brief Create a new index configuration which is a cross product of the
   * given configurations and merge it into the result.
   * result = result union (configuration1 * configuration2)
   * Ex: {I1} * {I23, I45} = {I123, I145}
   *
   * @param - configuration1: config1
   * @param - configuration2: config2
   * @param - result: cross product
   */
  void CrossProduct(const IndexConfiguration &configuration1,
                    const IndexConfiguration &configuration2,
                    IndexConfiguration &result);

  // Set of parsed and bound queries
  Workload query_set_;
  // Common context of index selection object.
  IndexSelectionContext context_;
  // Transaction.
  concurrency::TransactionContext *txn_;
};

}  // namespace brain
}  // namespace peloton
