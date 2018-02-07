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

namespace storage {
class StorageManager;
}  // namespace storage

namespace executor {

/**
 * @brief Stores information for one execution of a plan.
 */
class ExecutorContext {
 public:
  ExecutorContext(concurrency::TransactionContext *transaction,
                  codegen::QueryParameters parameters = {});

  DISALLOW_COPY_AND_MOVE(ExecutorContext);

  concurrency::TransactionContext *GetTransaction() const;

  const std::vector<type::Value> &GetParamValues() const;

  storage::StorageManager *GetStorageManager() const;

  codegen::QueryParameters &GetParams();

  type::EphemeralPool *GetPool();

  // Number of processed tuples during execution
  uint32_t num_processed = 0;

 private:
  // The transaction context
  concurrency::TransactionContext *transaction_;
  // All query parameters
  codegen::QueryParameters parameters_;
  // The storage manager instance
  storage::StorageManager *storage_manager_;
  // Temporary memory pool for allocations done during execution
  type::EphemeralPool pool_;
};

}  // namespace executor
}  // namespace peloton
