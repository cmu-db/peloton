//
// Created by Rui Wang on 16-4-4.
//

#pragma once

#include "backend/executor/abstract_executor.h"
#include "backend/executor/abstract_parallel_task_response.h"
#include "backend/common/blocking_queue.h"

namespace peloton {
namespace executor {

class AbstractExchangeExecutor {
public:
  AbstractExchangeExecutor(const AbstractExchangeExecutor &) = delete;
  AbstractExchangeExecutor &operator=(const AbstractExchangeExecutor&) = delete;
  AbstractExchangeExecutor(AbstractExchangeExecutor &&) = delete;
  AbstractExchangeExecutor &operator=(AbstractExchangeExecutor &&) = delete;

  explicit AbstractExchangeExecutor();

protected:
  BlockingQueue<AbstractParallelTaskResponse *> queue_;
};

}  // namespace executor
}  // namespace planner
