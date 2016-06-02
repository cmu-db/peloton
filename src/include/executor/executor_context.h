//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// executor_context.h
//
// Identification: src/executor/executor_context.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "concurrency/transaction.h"
#include "common/pool.h"
#include "common/value.h"

namespace peloton {
namespace executor {

//===--------------------------------------------------------------------===//
// Executor Context
//===--------------------------------------------------------------------===//

// TODO: We might move this flag into the types.h in the future
enum ParamsExecFlag {
  INVALID_FLAG,
  IN_NESTLOOP = 1 // nestloop (in+indexscan)
  //IN_**         // other types
};

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
  uint32_t GetParamsExecFlag() { return params_exec_flag_; }

  void SetParams(Value value) { params_.push_back(value); }
  void SetParamsExecFlag(ParamsExecFlag flag) { params_exec_flag_ = flag; }
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
  ParamsExecFlag params_exec_flag_ ;
};

}  // namespace executor
}  // namespace peloton
