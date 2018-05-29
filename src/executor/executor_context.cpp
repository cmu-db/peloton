//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// executor_context.cpp
//
// Identification: src/executor/executor_context.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "executor/executor_context.h"

#include "storage/storage_manager.h"

namespace peloton {
namespace executor {

////////////////////////////////////////////////////////////////////////////////
///
/// ExecutorContext
///
////////////////////////////////////////////////////////////////////////////////

ExecutorContext::ExecutorContext(concurrency::TransactionContext *transaction,
                                 codegen::QueryParameters parameters)
    : transaction_(transaction),
      parameters_(std::move(parameters)),
      storage_manager_(storage::StorageManager::GetInstance()),
      thread_states_(pool_) {}

concurrency::TransactionContext *ExecutorContext::GetTransaction() const {
  return transaction_;
}

const std::vector<type::Value> &ExecutorContext::GetParamValues() const {
  return parameters_.GetParameterValues();
}

storage::StorageManager &ExecutorContext::GetStorageManager() const {
  return *storage_manager_;
}

codegen::QueryParameters &ExecutorContext::GetParams() { return parameters_; }

type::EphemeralPool *ExecutorContext::GetPool() { return &pool_; }

ExecutorContext::ThreadStates &ExecutorContext::GetThreadStates() {
  return thread_states_;
}

////////////////////////////////////////////////////////////////////////////////
///
/// ThreadStates
///
////////////////////////////////////////////////////////////////////////////////

ExecutorContext::ThreadStates::ThreadStates(type::EphemeralPool &pool)
    : pool_(pool), num_threads_(0), state_size_(0), states_(nullptr) {}

void ExecutorContext::ThreadStates::Reset(const uint32_t state_size) {
  if (states_ != nullptr) {
    pool_.Free(states_);
    states_ = nullptr;
  }
  num_threads_ = 0;
  // Always fill out to nearest cache-line to prevent false sharing of states
  // between different threads.
  uint32_t pad = state_size & ~CACHELINE_SIZE;
  state_size_ = state_size + (pad != 0 ? CACHELINE_SIZE - pad : pad);
}

void ExecutorContext::ThreadStates::Allocate(const uint32_t num_threads) {
  PELOTON_ASSERT(state_size_ > 0);
  PELOTON_ASSERT(states_ == nullptr);
  num_threads_ = num_threads;
  uint32_t alloc_size = num_threads_ * state_size_;
  states_ = reinterpret_cast<char *>(pool_.Allocate(alloc_size));
  PELOTON_MEMSET(states_, 0, alloc_size);
}

char *ExecutorContext::ThreadStates::AccessThreadState(
    const uint32_t thread_id) const {
  PELOTON_ASSERT(state_size_ > 0);
  PELOTON_ASSERT(states_ != nullptr);
  PELOTON_ASSERT(thread_id < num_threads_);
  return states_ + (thread_id * state_size_);
}

}  // namespace executor
}  // namespace peloton
