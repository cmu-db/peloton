//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_scan_executor.h
//
// Identification: src/include/executor/index_scan_executor.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <vector>

#include "executor/abstract_scan_executor.h"
#include "planner/index_scan_plan.h"

namespace peloton {

namespace storage {
class AbstractTable;
}

namespace executor {

class IndexScanExecutor : public AbstractScanExecutor {
  IndexScanExecutor(const IndexScanExecutor &) = delete;
  IndexScanExecutor &operator=(const IndexScanExecutor &) = delete;

 public:
  explicit IndexScanExecutor(const planner::AbstractPlan *node,
                             ExecutorContext *executor_context);

  ~IndexScanExecutor();

 protected:
  bool DInit();

  bool DExecute();

 private:
  //===--------------------------------------------------------------------===//
  // Helper
  //===--------------------------------------------------------------------===//
  bool ExecPrimaryIndexLookup();
  bool ExecSecondaryIndexLookup();

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
  std::vector<common::Value> values_;

  std::vector<expression::AbstractExpression *> runtime_keys_;

  bool key_ready_ = false;
};

}  // namespace executor
}  // namespace peloton
