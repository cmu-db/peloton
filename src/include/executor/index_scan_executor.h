//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_scan_executor.h
//
// Identification: src/include/executor/index_scan_executor.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "executor/abstract_scan_executor.h"
#include "index/scan_optimizer.h"

namespace peloton {

namespace index {
class Index;
}

namespace storage {
class AbstractTable;
}

namespace executor {

/**
 * 2018-01-07: This is <b>deprecated</b>. Do not modify these classes.
 * The old interpreted engine will be removed.
 * @deprecated
 */
class IndexScanExecutor : public AbstractScanExecutor {
  IndexScanExecutor(const IndexScanExecutor &) = delete;
  IndexScanExecutor &operator=(const IndexScanExecutor &) = delete;

 public:
  explicit IndexScanExecutor(const planner::AbstractPlan *node,
                             ExecutorContext *executor_context);

  ~IndexScanExecutor();

  void UpdatePredicate(const std::vector<oid_t> &column_ids UNUSED_ATTRIBUTE,
                       const std::vector<type::Value> &values UNUSED_ATTRIBUTE);

  void ResetState();

 protected:
  bool DInit();

  bool DExecute();

 private:
  //===--------------------------------------------------------------------===//
  // Helper
  //===--------------------------------------------------------------------===//
  bool ExecPrimaryIndexLookup();
  bool ExecSecondaryIndexLookup();

  // When the required scan range has open boundaries, the tuples found by the
  // index might not be exact since the index can only give back tuples in a
  // close range. This function prune the head and the tail of the returned
  // tuple list to get the correct result.
  void CheckOpenRangeWithReturnedTuples(
      std::vector<ItemPointer> &tuple_locations);

  // Check whether the tuple at a given location satisfies the required
  // conditions on key columns
  bool CheckKeyConditions(const ItemPointer &tuple_location);

  //===--------------------------------------------------------------------===//
  // Executor State
  //===--------------------------------------------------------------------===//

  /** @brief Result of index scan. */
  std::vector<LogicalTile *> result_;

  /** @brief Result itr */
  oid_t result_itr_ = INVALID_OID;

  /** @brief Computed the result */
  bool done_ = false;

  //===--------------------------------------------------------------------===//
  // Plan Info
  //===--------------------------------------------------------------------===//

  /** @brief index associated with index scan. */
  std::shared_ptr<index::Index> index_;

  // the underlying table that the index is for
  const storage::AbstractTable *table_ = nullptr;

  // columns to be returned as results
  std::vector<oid_t> column_ids_;

  // columns for key accesses.
  std::vector<oid_t> key_column_ids_;

  // all the columns in a table.
  std::vector<oid_t> full_column_ids_;

  // expression types ( >, <, =, ...)
  std::vector<ExpressionType> expr_types_;

  // values for evaluation.
  std::vector<type::Value> values_;

  std::vector<expression::AbstractExpression *> runtime_keys_;

  bool key_ready_ = false;

  // whether the index scan range is left open
  bool left_open_ = false;

  // whether the index scan range is right open
  bool right_open_ = false;

  // copy from underlying plan
  index::IndexScanPredicate index_predicate_;

  // whether it is an order by + limit plan
  bool limit_ = false;

  // how many tuples should be returned
  int64_t limit_number_ = 0;

  // offset means from which point
  int64_t limit_offset_ = 0;

  // whether order by is descending
  bool descend_ = false;
};

}  // namespace executor
}  // namespace peloton
