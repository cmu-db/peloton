//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// update_executor.h
//
// Identification: src/include/executor/update_executor.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "executor/abstract_executor.h"
#include "expression/abstract_expression.h"
#include "planner/update_plan.h"

namespace peloton {
namespace executor {

class UpdateExecutor : public AbstractExecutor {
  UpdateExecutor(const UpdateExecutor &) = delete;
  UpdateExecutor &operator=(const UpdateExecutor &) = delete;

 public:
  explicit UpdateExecutor(const planner::AbstractPlan *node,
                          ExecutorContext *executor_context);

 protected:
  bool PerformUpdatePrimaryKey(bool is_owner, oid_t tile_group_id,
                               oid_t physical_tuple_id,
                               ItemPointer &old_location,
                               storage::TileGroup *&tile_group);

  bool DInit();

  bool DExecute();

 private:
  storage::DataTable *target_table_ = nullptr;
  const planner::ProjectInfo *project_info_ = nullptr;
};

}  // namespace executor
}  // namespace peloton
