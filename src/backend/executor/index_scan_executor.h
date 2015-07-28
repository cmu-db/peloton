/**
 * @brief Header file for index scan executor.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "backend/executor/abstract_executor.h"
#include "backend/planner/index_scan_node.h"

#include <vector>

namespace peloton {
namespace executor {

class IndexScanExecutor : public AbstractExecutor {
  IndexScanExecutor(const IndexScanExecutor &) = delete;
  IndexScanExecutor &operator=(const IndexScanExecutor &) = delete;

 public:
  explicit IndexScanExecutor(planner::AbstractPlanNode *node,
                             ExecutorContext *executor_context);

 protected:
  bool DInit();

  bool DExecute();

 private:
  //===--------------------------------------------------------------------===//
  // Executor State
  //===--------------------------------------------------------------------===//

  /** @brief Result of index scan. */
  std::vector<LogicalTile *> result;

  /** @brief Result itr */
  oid_t result_itr = INVALID_OID;

  /** @brief Computed the result */
  bool done = false;

  //===--------------------------------------------------------------------===//
  // Plan Info
  //===--------------------------------------------------------------------===//

  /** @brief index associated with index scan. */
  index::Index *index_ = nullptr;

  /** @brief starting key for index scan. */
  const storage::Tuple *start_key_ = nullptr;

  /** @brief ending key for index scan. */
  const storage::Tuple *end_key_ = nullptr;

  /** @brief whether we also need to include the terminal values ?
   *  Like ID > 50 (not inclusive) or ID >= 50 (inclusive)
   * */
  bool start_inclusive_ = false;

  bool end_inclusive_ = false;

  /** @brief Columns from tile group to be added to logical tile output. */
  std::vector<oid_t> column_ids_;
};

}  // namespace executor
}  // namespace peloton
