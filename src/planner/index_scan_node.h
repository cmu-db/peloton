/**
 * @brief Header for index scan plan node.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/types.h"
#include "expression/abstract_expression.h"
#include "planner/abstract_plan_node.h"

namespace nstore {

namespace index {
class Index;
}

namespace storage{
class Tuple;
}

namespace planner {

class IndexScanNode : public AbstractPlanNode {
 public:
  IndexScanNode(const IndexScanNode &) = delete;
  IndexScanNode& operator=(const IndexScanNode &) = delete;
  IndexScanNode(IndexScanNode &&) = delete;
  IndexScanNode& operator=(IndexScanNode &&) = delete;

  IndexScanNode(
      index::Index *index,
      storage::Tuple *start_key,
      storage::Tuple *end_key,
      bool inclusive,
      const std::vector<oid_t> &column_ids)
    : index_(index),
      start_key_(start_key),
      end_key_(end_key),
      inclusive_(inclusive),
      column_ids_(column_ids) {
  }

  const index::Index* GetIndex() const{
    return index_;
  }

  const storage::Tuple* GetStartKey() const {
    return start_key_;
  }

  const storage::Tuple* GetEndKey() const {
    return end_key_;
  }

  bool IsInclusive() const {
    return inclusive_;
  }

  const std::vector<oid_t>& GetColumnIds() const {
    return column_ids_;
  }

  inline PlanNodeType GetPlanNodeType() const {
    return PLAN_NODE_TYPE_INDEXSCAN;
  }

  inline std::string GetInfo() const {
    return "IndexScan";
  }

 private:

  /** @brief index associated with index scan. */
  index::Index *index_;

  /** @brief starting key for index scan. */
  storage::Tuple *start_key_;

  /** @brief ending key for index scan. */
  storage::Tuple *end_key_;

  /** @brief whether we also need to include the terminal values ?
   *  Like ID > 50 (not inclusive) or ID >= 50 (inclusive)
   * */
  bool inclusive_;

  /** @brief Columns from tile group to be added to logical tile output. */
  const std::vector<oid_t> column_ids_;
};

} // namespace planner
} // namespace nstore
