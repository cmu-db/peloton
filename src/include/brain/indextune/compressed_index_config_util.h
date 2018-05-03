//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// compressed_index_config.h
//
// Identification: src/include/brain/indextune/compressed_index_config.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <boost/dynamic_bitset.hpp>
#include "brain/index_selection.h"
#include "catalog/catalog.h"
#include "catalog/database_catalog.h"
#include "catalog/index_catalog.h"
#include "catalog/table_catalog.h"
#include "concurrency/transaction_manager_factory.h"
#include "brain/util/eigen_util.h"
#include "planner/plan_util.h"
#include "brain/indextune/compressed_index_config.h"

namespace peloton {
namespace brain {

class CompressedIndexConfigUtil {
 public:
  /**
 * Given a SQLStatementList, generate the prefix closure from the first
 * SQLStatement element
 * @param container: input container
 * @param query: query in question
 * @return the prefix closure as a bitset
 */
  static void AddCandidates(
      CompressedIndexConfigContainer &container,
      const std::string &query,
      boost::dynamic_bitset<>& add_candidates);

  /**
  * Given a SQLStatement, generate drop candidates
  * @param container: input container
  * @param sql_stmt: the SQLStatement
  * @return the drop candidates
  */
  static void DropCandidates(
      CompressedIndexConfigContainer &container,
      const std::string &query,
      boost::dynamic_bitset<>& drop_candidates);

  /**
  * @brief Return a bitset initialized using a list of indexes
  */
  static std::unique_ptr<boost::dynamic_bitset<>> GenerateBitSet(
      const CompressedIndexConfigContainer &container,
      const std::vector<std::shared_ptr<brain::IndexObject>> &idx_objs);

  static void SetBit(const CompressedIndexConfigContainer &container,
                boost::dynamic_bitset<> &bitmap,
                const std::shared_ptr<IndexObject> &idx_object);

  /**
 * Get the covered index configuration feature vector.
 * The difference between this and `GetCurrentIndexConfig` is that
 * all single column index configurations by a multicolumn index are
 * considered covered and set to 1.
 * @param config_vec: configuration vector to construct
 */
  static void ConstructConfigFeature(const CompressedIndexConfigContainer& container,
                                     vector_eig &config_vec);
  // Feature constructors
  /**
   * Constructs the feature vector representing the SQL query running on the
   * current
   * index configuration. This is done by using the following feature vector:
   * = 0.0 if not in f(query)
   * = 1.0 if in f(query) and belongs to current config
   * = -1 if in f(query) but not in current config
   * where f(query) is first recommended_index(query)(0->n), then
   * drop_index(query)(n->2*n)
   * @param add_candidates: add candidate suggestions
   * @param drop_candidates: drop candidate suggestions
   * @param query_config_vec: query configuration vector to construct
   * // TODO: not in f(query) should split into:  (i)!f(query) &&
   * belongs(config) (ii) !(f(query) && belongs(config))?
   */
  static void ConstructQueryConfigFeature(
      const boost::dynamic_bitset<>& curr_config_set,
      const boost::dynamic_bitset<> &add_candidate_set,
      const boost::dynamic_bitset<> &drop_candidate_set,
      vector_eig &query_config_vec);
 private:
  /**
   * @brief: converts query string to a binded sql-statement list
   */
  static std::unique_ptr<parser::SQLStatementList> ToBindedSqlStmtList(
      CompressedIndexConfigContainer &container, const std::string &query_string);

  /**
   * @brief Convert an index triplet to an index object
   */
  static std::shared_ptr<brain::IndexObject> ConvertIndexTriplet(
      CompressedIndexConfigContainer &container,
      const planner::col_triplet &idx_triplet);
};
}
}
