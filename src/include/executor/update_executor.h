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

 private:
  storage::DataTable *target_table_ = nullptr;
  const planner::ProjectInfo *project_info_ = nullptr;
};

}  // namespace executor
}  // namespace peloton
