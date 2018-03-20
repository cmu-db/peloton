//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_context_proxy.cpp
//
// Identification: src/codegen/proxy/transaction_context_proxy.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/transaction_context_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_TYPE(TransactionContext, "peloton::concurrency::TransactionContext", MEMBER(opaque));

}  // namespace codegen
}  // namespace peloton