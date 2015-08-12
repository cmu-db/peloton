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

#include "postmaster/peloton.h"

#include "postgres.h"
#include "c.h"
#include "nodes/nodes.h"

namespace peloton {
namespace bridge {

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

  static void ProcessUtility(Node *parsetree,DDL_Info* ddl_info,
                             Peloton_Status *status, TransactionId txn_id);
};

}  // namespace bridge
}  // namespace peloton
