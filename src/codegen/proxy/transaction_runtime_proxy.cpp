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

/// PerformVectorizedRead()
DEFINE_METHOD(TransactionRuntime, PerformVectorizedRead,
              &peloton::codegen::TransactionRuntime::PerformVectorizedRead,
              "_ZN7peloton7codegen18TransactionRuntime21PerformVectorizedReadE"
              "RNS_11concurrency11TransactionERNS_7storage9TileGroupEjjPj");

/// PerformDelete()
DEFINE_METHOD(TransactionRuntime, PerformDelete,
              &peloton::codegen::TransactionRuntime::PerformDelete,
              "_ZN7peloton7codegen18TransactionRuntime13PerformDeleteERNS_"
              "11concurrency11TransactionERNS_7storage9DataTableEjj");

/// IncreaseNumProcessed()
DEFINE_METHOD(TransactionRuntime, IncreaseNumProcessed,
              &peloton::codegen::TransactionRuntime::IncreaseNumProcessed,
              "_ZN7peloton7codegen18TransactionRuntime20IncreaseNumProcessedEP"
              "NS_8executor15ExecutorContextE");

}  // namespace codegen
}  // namespace peloton