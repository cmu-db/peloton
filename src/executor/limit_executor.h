/**
 * @brief Header file for limit executor.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "common/types.h"
#include "executor/abstract_executor.h"

namespace nstore {
namespace executor {

class LimitExecutor: public AbstractExecutor {
public:
  LimitExecutor(const LimitExecutor &) = delete;
  LimitExecutor& operator=(const LimitExecutor &) = delete;
  LimitExecutor(const LimitExecutor &&) = delete;
  LimitExecutor& operator=(const LimitExecutor &&) = delete;

  explicit LimitExecutor(planner::AbstractPlanNode *node,
                         Transaction *transaction);

protected:
  bool DInit();

  bool DExecute();

private:
  size_t num_skipped_ = 0;
  size_t num_returned_ = 0;

};

} /* namespace executor */
} /* namespace nstore */
