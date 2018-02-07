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

DEFINE_TYPE(ExecutorContext, "executor::ExecutorContext", MEMBER(num_processed),
            MEMBER(txn), MEMBER(params), MEMBER(storage_manager), MEMBER(pool));

}  // namespace codegen
}  // namespace peloton