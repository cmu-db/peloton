//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_pool.h
//
// Identification: src/concurrency/transaction_pool.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_pool.h"

namespace peloton {
namespace concurrency {

size_t TransactionPool::max_concurrency_ = 10;

}  // End concurrency namespace
}  // End peloton namespace
