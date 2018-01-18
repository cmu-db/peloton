//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// executor_context.h
//
// Identification: src/include/executor/executor_context.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/query_parameters.h"
#include "type/ephemeral_pool.h"
#include "type/value.h"

namespace peloton {

namespace concurrency {
class TransactionContext;
}  // namespace concurrency

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

  type::EphemeralPool *GetPool();

  // Number of processed tuples during execution
  uint32_t num_processed = 0;

 private:
  // The transaction context
  concurrency::TransactionContext *transaction_;
  // All query parameters
  codegen::QueryParameters parameters_;
  // Temporary memory pool for allocations done during execution
  std::unique_ptr<type::EphemeralPool> pool_;
};

}  // namespace executor
}  // namespace peloton
