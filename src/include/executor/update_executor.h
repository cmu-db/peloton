//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// update_executor.h
//
// Identification: src/include/executor/update_executor.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "executor/abstract_executor.h"
#include "expression/abstract_expression.h"
#include "planner/update_plan.h"

namespace peloton {

namespace storage {
class TileGroup;
class TileGroupHeader;
}

namespace executor {

/**
 * 2018-01-07: This is <b>deprecated</b>. Do not modify these classes.
 * The old interpreted engine will be removed.
 * @deprecated
 */
class UpdateExecutor : public AbstractExecutor {
  UpdateExecutor(const UpdateExecutor &) = delete;
  UpdateExecutor &operator=(const UpdateExecutor &) = delete;

 public:
  explicit UpdateExecutor(const planner::AbstractPlan *node,
                          ExecutorContext *executor_context);

 private:
  bool PerformUpdatePrimaryKey(bool is_owner, 
                               storage::TileGroup *tile_group,
                               storage::TileGroupHeader *tile_group_header, 
                               oid_t physical_tuple_id,
                               ItemPointer &old_location);

  bool DInit();

  bool DExecute();

  inline bool IsInStatementWriteSet(ItemPointer &location) {
    return (statement_write_set_.find(location) != statement_write_set_.end());
  }

 private:
  storage::DataTable *target_table_ = nullptr;
  const planner::ProjectInfo *project_info_ = nullptr;

  // Write set for tracking newly created tuples inserted by the same statement
  // This statement-level write set is essential for avoiding the Halloween Problem,
  // which refers to the phenomenon that an update operation causes a change to
  // a tuple, potentially allowing this tuple to be visited more than once during
  // the same operation.
  // By maintaining the statement-level write set, an update operation will check 
  // whether the to-be-updated tuple is created by the same operation.
  WriteSet statement_write_set_;
};

}  // namespace executor
}  // namespace peloton
