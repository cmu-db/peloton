//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// executor_context_proxy.h
//
// Identification: src/codegen/proxy/executor_context_proxy.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/executor_context_proxy.h"
#include "codegen/proxy/transaction_context_proxy.h"

namespace peloton {
namespace codegen {

// ThreadStates
DEFINE_TYPE(ThreadStates, "executor::ThreadStates", MEMBER(pool),
            MEMBER(num_threads), MEMBER(state_size), MEMBER(states));

DEFINE_METHOD(peloton::executor::ExecutorContext, ThreadStates, Reset);
DEFINE_METHOD(peloton::executor::ExecutorContext, ThreadStates, Allocate);

// ExecutorContext
DEFINE_TYPE(ExecutorContext, "executor::ExecutorContext", MEMBER(num_processed),
            MEMBER(txn), MEMBER(params), MEMBER(storage_manager), MEMBER(pool),
            MEMBER(thread_states));

}  // namespace codegen
}  // namespace peloton