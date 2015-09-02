//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// ddl.h
//
// Identification: src/backend/bridge/ddl/ddl.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/concurrency/transaction_manager.h"

#include "nodes/nodes.h"

#include <mutex>

namespace peloton {
namespace bridge {

static std::mutex parsetree_stack_mutex;

//===--------------------------------------------------------------------===//
// DDL
//===--------------------------------------------------------------------===//

class DDL {
 public:
  DDL(const DDL &) = delete;
  DDL &operator=(const DDL &) = delete;
  DDL(DDL &&) = delete;
  DDL &operator=(DDL &&) = delete;

  //===--------------------------------------------------------------------===//
  // Utilities
  //===--------------------------------------------------------------------===//

  static void ProcessUtility(Node *parsetree,
                             concurrency::TransactionId txn_id);
};

}  // namespace bridge
}  // namespace peloton
