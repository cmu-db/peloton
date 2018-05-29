//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// executor_context_proxy.h
//
// Identification: src/include/codegen/proxy/executor_context_proxy.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"
#include "codegen/proxy/transaction_context_proxy.h"
#include "codegen/proxy/query_parameters_proxy.h"
#include "codegen/proxy/storage_manager_proxy.h"
#include "codegen/proxy/pool_proxy.h"
#include "executor/executor_context.h"

namespace peloton {
namespace codegen {

PROXY(ThreadStates) {
  DECLARE_MEMBER(0, peloton::type::EphemeralPool *, pool);
  DECLARE_MEMBER(1, uint32_t, num_threads);
  DECLARE_MEMBER(2, uint32_t, state_size);
  DECLARE_MEMBER(3, char *, states);
  DECLARE_TYPE;

  DECLARE_METHOD(Reset);
  DECLARE_METHOD(Allocate);
};

PROXY(ExecutorContext) {
  /// We don't need access to internal fields, so use an opaque byte array
  DECLARE_MEMBER(0, uint32_t, num_processed);
  DECLARE_MEMBER(1, concurrency::TransactionContext *, txn);
  DECLARE_MEMBER(2, codegen::QueryParameters, params);
  DECLARE_MEMBER(3, storage::StorageManager *, storage_manager);
  DECLARE_MEMBER(4, peloton::type::EphemeralPool, pool);
  DECLARE_MEMBER(5, executor::ExecutorContext::ThreadStates, thread_states);
  DECLARE_TYPE;
};

TYPE_BUILDER(ThreadStates, executor::ExecutorContext::ThreadStates);
TYPE_BUILDER(ExecutorContext, executor::ExecutorContext);

}  // namespace codegen
}  // namespace peloton
