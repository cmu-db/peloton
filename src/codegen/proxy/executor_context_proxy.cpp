//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// executor_context_proxy.h
//
// Identification: src/codegen/proxy/executor_context_proxy.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/executor_context_proxy.h"
#include "codegen/proxy/transaction_context_proxy.h"

namespace peloton {
namespace codegen {

// ThreadStates
DEFINE_TYPE(ThreadStates, "executor::ThreadStates", pool, num_threads,
            state_size, states);

DEFINE_METHOD(peloton::executor::ExecutorContext, ThreadStates, Reset);
DEFINE_METHOD(peloton::executor::ExecutorContext, ThreadStates, Allocate);

// ExecutorContext
DEFINE_TYPE(ExecutorContext, "executor::ExecutorContext", num_processed, txn,
            params, storage_manager, pool, thread_states);

}  // namespace codegen
}  // namespace peloton