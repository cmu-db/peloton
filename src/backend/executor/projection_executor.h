//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// projection_executor.h
//
// Identification: src/backend/executor/projection_executor.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/executor/abstract_executor.h"
#include "backend/planner/project_info.h"

namespace peloton {
namespace executor {

class ProjectionExecutor : public AbstractExecutor {
 public:
  ProjectionExecutor(const ProjectionExecutor &) = delete;
  ProjectionExecutor &operator=(const ProjectionExecutor &) = delete;
  ProjectionExecutor(const ProjectionExecutor &&) = delete;
  ProjectionExecutor &operator=(const ProjectionExecutor &&) = delete;

  explicit ProjectionExecutor(const planner::AbstractPlan *node,
                              ExecutorContext *executor_context);

 protected:
  bool DInit();

  bool DExecute();

 private:
  //===--------------------------------------------------------------------===//
  // Executor State
  //===--------------------------------------------------------------------===//

  /** @brief Projection Info.            */
  const planner::ProjectInfo *project_info_ = nullptr;

  /** @brief Schema of projected tuples. */
  const catalog::Schema *schema_ = nullptr;
};

} /* namespace executor */
} /* namespace peloton */
