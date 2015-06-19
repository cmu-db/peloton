/**
 * @brief Header file for limit executor.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "backend/common/types.h"
#include "backend/executor/abstract_executor.h"

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

  //===--------------------------------------------------------------------===//
  // Executor State
  //===--------------------------------------------------------------------===//

  /** @brief Number of tuples skipped. */
  oid_t num_skipped_ = INVALID_OID;

  /** @brief Number of tuples returned. */
  oid_t num_returned_ = INVALID_OID;

};

} /* namespace executor */
} /* namespace nstore */
