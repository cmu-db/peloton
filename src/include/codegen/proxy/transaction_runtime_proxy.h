//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_runtime_proxy.h
//
// Identification: src/include/codegen/proxy/transaction_runtime_proxy.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"

namespace peloton {
namespace codegen {

PROXY(TransactionRuntime) {
  /// We only need to proxy PerformVectorizedRead()
  /// in codegen::TransactionRuntime.
  DECLARE_METHOD(PerformVectorizedRead);
};

}  // namespace codegen
}  // namespace peloton
