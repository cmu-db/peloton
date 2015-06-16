/**
 * @brief Header file for sequential scan executor.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "common/types.h"
#include "executor/abstract_executor.h"
#include "planner/seq_scan_node.h"

namespace nstore {
namespace executor {

class SeqScanExecutor : public AbstractExecutor {
 public:
  SeqScanExecutor(const SeqScanExecutor &) = delete;
  SeqScanExecutor& operator=(const SeqScanExecutor &) = delete;
  SeqScanExecutor(SeqScanExecutor &&) = delete;
  SeqScanExecutor& operator=(SeqScanExecutor &&) = delete;

  explicit SeqScanExecutor(planner::AbstractPlanNode *node,
                           Transaction *transaction);

 protected:
  bool DInit();

  bool DExecute();

 private:

  //===--------------------------------------------------------------------===//
  // Executor State
  //===--------------------------------------------------------------------===//

  /** @brief Keeps track of current tile group id being scanned. */
  oid_t current_tile_group_id_ = 0;

  //===--------------------------------------------------------------------===//
  // Plan Info
  //===--------------------------------------------------------------------===//

  /** @brief Pointer to table to scan from. */
  const storage::DataTable *table_ = nullptr;

  /** @brief Selection predicate. */
  const expression::AbstractExpression *predicate_ = nullptr;

  /** @brief Columns from tile group to be added to logical tile output. */
  std::vector<oid_t> column_ids_;

};

} // namespace executor
} // namespace nstore
