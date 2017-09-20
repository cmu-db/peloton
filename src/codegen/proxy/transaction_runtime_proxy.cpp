//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_runtime_proxy.cpp
//
// Identification: src/codegen/proxy/transaction_runtime_proxy.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/transaction_runtime_proxy.h"

#include "codegen/transaction_runtime.h"
#include "codegen/proxy/data_table_proxy.h"
#include "codegen/proxy/executor_context_proxy.h"
#include "codegen/proxy/transaction_proxy.h"
#include "codegen/proxy/tile_group_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_METHOD(peloton::codegen, TransactionRuntime, PerformVectorizedRead);
DEFINE_METHOD(peloton::codegen, TransactionRuntime, PerformDelete);
DEFINE_METHOD(peloton::codegen, TransactionRuntime, IncreaseNumProcessed);

}  // namespace codegen
}  // namespace peloton
