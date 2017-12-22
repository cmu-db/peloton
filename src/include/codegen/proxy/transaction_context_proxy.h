//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_context_proxy.h
//
// Identification: src/include/codegen/proxy/transaction_context_proxy.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"
#include "codegen/proxy/type_builder.h"
#include "concurrency/transaction_context.h"

namespace peloton {
namespace codegen {

PROXY(TransactionContext) {
  DECLARE_MEMBER(0, char[sizeof(concurrency::TransactionContext)], opaque);
  DECLARE_TYPE;
};

TYPE_BUILDER(TransactionContext, concurrency::TransactionContext);

}  // namespace codegen
}  // namespace peloton