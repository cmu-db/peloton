//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "concurrency/transaction_manager.h"

namespace peloton {
namespace concurrency {

// Current transaction for the backend thread
thread_local Transaction *current_txn;

}  // End concurrency namespace
}  // End peloton namespace
