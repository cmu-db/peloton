//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// executor_context.h
//
// Identification: src/include/executor/executor_context.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/query_parameters.h"
#include "type/ephemeral_pool.h"
#include "type/value.h"

namespace peloton {

namespace concurrency {
class TransactionContext;
}

namespace executor {

/**
 * @brief Stores information for one execution of a plan.
 */
class ExecutorContext {
 public:
  explicit ExecutorContext(concurrency::TransactionContext *transaction,
                           codegen::QueryParameters parameters = {});

  DISALLOW_COPY_AND_MOVE(ExecutorContext);

  ~ExecutorContext() = default;

  concurrency::TransactionContext *GetTransaction() const;

  const std::vector<type::Value> &GetParamValues() const;

  codegen::QueryParameters &GetParams();

  // OBSOLETE: This is for the interpreted engine.
  type::EphemeralPool *GetPool();

  // num of tuple processed
  uint32_t num_processed = 0;

 private:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  concurrency::TransactionContext *transaction_;
  codegen::QueryParameters parameters_;

  // OBSOLETE: This is for the interpreted engine.
  std::unique_ptr<type::EphemeralPool> pool_;
};

}  // namespace executor
}  // namespace peloton
