//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// executor_context.h
//
// Identification: src/backend/executor/executor_context.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/concurrency/transaction.h"
#include "backend/common/value_vector.h"
#include "backend/common/pool.h"

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

  ExecutorContext(concurrency::Transaction *transaction)
      : transaction_(transaction) {}

  ExecutorContext(concurrency::Transaction *transaction,
                  const ValueArray &params)
      : transaction_(transaction), params_(params) {}

  concurrency::Transaction *GetTransaction() const { return transaction_; }

  const ValueArray &GetParams() const { return params_; }

  void SetPool(VarlenPool *pool) {
    pool_ = pool;
  }

  VarlenPool *GetPool() const {
    return pool_;
  }

  ~ExecutorContext(){
    for(int i=0; i < params_.GetSize(); i++){
      params_[i].Free();
    }
  }

  // num of tuple processed
  uint32_t num_processed = 0;

 private:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // transaction
  concurrency::Transaction *transaction_;

  // params
  ValueArray params_;

  // pool
  VarlenPool *pool_ = nullptr;
};

}  // namespace executor
}  // namespace peloton
