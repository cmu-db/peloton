//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// index_scan_executor.h
//
// Identification: src/backend/executor/index_scan_executor.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "backend/executor/abstract_scan_executor.h"
#include "backend/planner/index_scan_plan.h"

namespace peloton {

namespace storage{
class AbstractTable;
}

namespace executor {

class IndexScanExecutor : public AbstractScanExecutor {
  IndexScanExecutor(const IndexScanExecutor &) = delete;
  IndexScanExecutor &operator=(const IndexScanExecutor &) = delete;

 public:
  explicit IndexScanExecutor(const planner::AbstractPlan *node,
                             ExecutorContext *executor_context);

 protected:
  bool DInit();

  bool DExecute();

 private:
  //===--------------------------------------------------------------------===//
  // Helper
  //===--------------------------------------------------------------------===//
  bool ExecIndexLookup();

  void ExecProjection();

  void ExecPredication();

  //===--------------------------------------------------------------------===//
  // Executor State
  //===--------------------------------------------------------------------===//

  /** @brief Result of index scan. */
  std::vector<LogicalTile *> result;

  /** @brief Result itr */
  oid_t result_itr = INVALID_OID;

  /** @brief Computed the result */
  bool done_ = false;

  //===--------------------------------------------------------------------===//
  // Plan Info
  //===--------------------------------------------------------------------===//

  /** @brief index associated with index scan. */
  index::Index *index_ = nullptr;

  const storage::AbstractTable *table_ = nullptr;

  std::vector<oid_t> column_ids_;

  std::vector<oid_t> key_column_ids_;

  std::vector<ExpressionType> expr_types_;

  std::vector<peloton::Value> values_;

  std::vector<expression::AbstractExpression *> runtime_keys_;

  std::vector<oid_t> full_column_ids_;

  bool key_ready = false;
};

}  // namespace executor
}  // namespace peloton
