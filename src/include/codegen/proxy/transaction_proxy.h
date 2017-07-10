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

#include "codegen/codegen.h"
#include "codegen/proxy/proxy.h"
#include "concurrency/transaction.h"

namespace peloton {
namespace codegen {

PROXY(Transaction) {
  PROXY_MEMBER_FIELD(0, char[sizeof(concurrency::Transaction)], opaque);
  PROXY_TYPE("peloton::concurrency::Transaction",
             char[sizeof(concurrency::Transaction)]);
};

namespace proxy {
template <>
struct TypeBuilder<::peloton::concurrency::Transaction> {
  using Type = llvm::Type *;
  static Type GetType(CodeGen &codegen) {
    return TransactionProxy::GetType(codegen);
  }
};
}  // namespace proxy

}  // namespace codegen
}  // namespace peloton
