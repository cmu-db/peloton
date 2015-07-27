/*-------------------------------------------------------------------------
 *
 * abstract_scan.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/executor/abstract_scan.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/common/types.h"
#include "backend/executor/abstract_executor.h"
#include "backend/planner/abstract_scan_node.h"

namespace peloton {
namespace executor {

class SeqScanExecutor : public AbstractExecutor {
 public:
  SeqScanExecutor(const SeqScanExecutor &) = delete;
  SeqScanExecutor &operator=(const SeqScanExecutor &) = delete;
  SeqScanExecutor(SeqScanExecutor &&) = delete;
  SeqScanExecutor &operator=(SeqScanExecutor &&) = delete;

  explicit SeqScanExecutor(planner::AbstractPlanNode *node,
                           ExecutorContext *executor_context);

 protected:
  bool DInit();

  bool DExecute();

 private:
  //===--------------------------------------------------------------------===//
  // Executor State
  //===--------------------------------------------------------------------===//

  /** @brief Keeps track of current tile group id being scanned. */
  oid_t current_tile_group_offset_ = INVALID_OID;

  /** @brief Keeps track of the number of tile groups to scan. */
  oid_t table_tile_group_count_ = INVALID_OID;

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

}  // namespace executor
}  // namespace peloton
