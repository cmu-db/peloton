//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_proxy.cpp
//
// Identification: src/codegen/proxy/transaction_proxy.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/transaction_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_TYPE(Transaction, "peloton::concurrency::Transaction", MEMBER(opaque));

}  // namespace codegen
}  // namespace peloton