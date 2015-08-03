/**
 * @brief Header for postgres ddl.
 *
 * Copyright(c) 2015, CMU
 */

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

  static void ProcessUtility(Node *parsetree, 
                             const char *queryString,
                             Peloton_Status* status,
                             TransactionId txn_id);
};

}  // namespace bridge
}  // namespace peloton
