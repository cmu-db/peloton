/**
 * @brief Header for postgres ddl.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "backend/catalog/constraint.h"

#include "postgres.h"
#include "c.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// DDL
//===--------------------------------------------------------------------===//

class DDL {

 public:
  DDL(const DDL &) = delete;
  DDL& operator=(const DDL &) = delete;
  DDL(DDL &&) = delete;
  DDL& operator=(DDL &&) = delete;

  //===--------------------------------------------------------------------===//
  // Utilities
  //===--------------------------------------------------------------------===//

  static void ProcessUtility(Node *parsetree,
                             const char *queryString);

  //Add the constraint to the table
  static bool AddConstraint(Oid relation_oid,
                            Constraint* constraint);

};

} // namespace bridge
} // namespace peloton

