//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// executor_context.h
//
// Identification: src/include/executor/executor_context.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "type/ephemeral_pool.h"
#include "type/value.h"

namespace peloton {

// class Value;

namespace concurrency {
class Transaction;
}

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
                  const std::vector<type::Value> &params);

  ~ExecutorContext();

  concurrency::Transaction *GetTransaction() const;

  const std::vector<type::Value> &GetParams() const;

  void SetParams(type::Value &value);

  void ClearParams();

  // Get a pool
  type::EphemeralPool *GetPool();

  // num of tuple processed
  uint32_t num_processed = 0;

 private:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // transaction
  concurrency::Transaction *transaction_;

  // params
  std::vector<type::Value> params_;

  // pool
  std::unique_ptr<type::EphemeralPool> pool_;
};

}  // namespace executor
}  // namespace peloton
