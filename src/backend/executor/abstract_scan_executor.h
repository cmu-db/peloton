//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// abstract_scan_executor.h
//
// Identification: src/backend/executor/abstract_scan_executor.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/planner/abstract_scan_plan.h"
#include "backend/common/types.h"
#include "backend/executor/abstract_executor.h"

namespace peloton {
namespace executor {

/**
 * Super class for different kinds of scan executor.
 * It provides common codes for all kinds of scan:
 * evaluate generic predicates and simple projections.
 */
class AbstractScanExecutor : public AbstractExecutor {
 public:
  AbstractScanExecutor(const AbstractScanExecutor &) = delete;
  AbstractScanExecutor &operator=(const AbstractScanExecutor &) = delete;
  AbstractScanExecutor(AbstractScanExecutor &&) = delete;
  AbstractScanExecutor &operator=(AbstractScanExecutor &&) = delete;

  explicit AbstractScanExecutor(planner::AbstractPlan *node,
                                ExecutorContext *executor_context);

 protected:
  bool DInit();

  bool DExecute() = 0;

 protected:
  //===--------------------------------------------------------------------===//
  // Plan Info
  //===--------------------------------------------------------------------===//

  /** @brief Selection predicate. */
  const expression::AbstractExpression *predicate_ = nullptr;

  /** @brief Columns from tile group to be added to logical tile output. */
  std::vector<oid_t> column_ids_;
};

}  // namespace executor
}  // namespace peloton
