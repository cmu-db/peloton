//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_proxy.h
//
// Identification: src/include/codegen/proxy/transaction_proxy.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"
#include "codegen/proxy/type_builder.h"
#include "concurrency/transaction.h"

namespace peloton {
namespace codegen {

PROXY(Transaction) {
  DECLARE_MEMBER(0, char[sizeof(concurrency::Transaction)], opaque);
  DECLARE_TYPE;
};

TYPE_BUILDER(Transaction, concurrency::Transaction);

}  // namespace codegen
}  // namespace peloton
