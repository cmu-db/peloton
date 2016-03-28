//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// executor_context.h
//
// Identification: src/backend/executor/executor_context.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/concurrency/transaction.h"
#include "backend/common/pool.h"
#include "backend/common/value.h"

namespace peloton {
namespace executor {

//===--------------------------------------------------------------------===//
// Executor Context
//===--------------------------------------------------------------------===//

class ExecutorContext {
 public:
  ExecutorContext(const ExecutorContext &) = delete;
  ExecutorContext &operator=(const ExecutorContext &) = delete;
  ExecutorContext(ExecutorContext &&) = delete;
  ExecutorContext &operator=(ExecutorContext &&) = delete;

  ExecutorContext(concurrency::Transaction *transaction);

  ExecutorContext(concurrency::Transaction *transaction,
                  const std::vector<Value> &params);

  ~ExecutorContext();

  concurrency::Transaction *GetTransaction() const { return transaction_; }

  const std::vector<Value> &GetParams() const { return params_; }
  uint32_t GetParamsExec() { return params_exec_; }

  void SetParams(Value value) { params_.push_back(value); }
  void SetParamsExec(uint32_t flag) { params_exec_ = flag; }
  void ClearParams() { params_.clear(); }

  // Get a varlen pool (will construct the pool only if needed)
  VarlenPool *GetExecutorContextPool();

  // num of tuple processed
  uint32_t num_processed = 0;

 private:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // transaction
  concurrency::Transaction *transaction_;

  // params
  std::vector<Value> params_;

  // pool
  std::unique_ptr<VarlenPool> pool_;

  // PARAMS_EXEC_Flag
  /*
   * 1 IN: nestloop+indexscan
   * 2 unknown yet
   * 3 unknown yet
   */
  uint32_t params_exec_ = 0;  // 1 IN: nestloop+indexscan
};

}  // namespace executor
}  // namespace peloton
