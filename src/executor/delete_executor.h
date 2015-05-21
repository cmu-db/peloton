/**
 * @brief Header file for delete executor.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "executor/abstract_executor.h"

#include <vector>

namespace nstore {
namespace executor {

class DeleteExecutor : public AbstractExecutor {
 public:
  DeleteExecutor(const DeleteExecutor &) = delete;
  DeleteExecutor& operator=(const DeleteExecutor &) = delete;
  DeleteExecutor(DeleteExecutor &&) = delete;
  DeleteExecutor& operator=(DeleteExecutor &&) = delete;

  DeleteExecutor(planner::AbstractPlanNode *node,
                 Transaction *transaction);

  ~DeleteExecutor(){}

 protected:
  bool DInit();

  bool DExecute();

};

} // namespace executor
} // namespace nstore
