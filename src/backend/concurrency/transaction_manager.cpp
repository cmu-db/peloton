//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_manager.cpp
//
// Identification: src/backend/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "transaction_manager.h"

namespace peloton {
namespace concurrency {

// Current transaction for the backend thread
thread_local Transaction *current_txn;

}  // End storage namespace
}  // End peloton namespace
