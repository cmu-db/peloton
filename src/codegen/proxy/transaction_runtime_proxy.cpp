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
#include "codegen/proxy/transaction_context_proxy.h"
#include "codegen/proxy/tile_group_proxy.h"
#include "codegen/proxy/value_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_METHOD(peloton::codegen, TransactionRuntime, PerformVectorizedRead);

}  // namespace codegen
}  // namespace peloton
