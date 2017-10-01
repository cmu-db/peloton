//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// variable_set_statement.h
//
// Identification: src/include/parser/variable_set_statement.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "common/logger.h"
#include "common/sql_node_visitor.h"
#include "parser/sql_statement.h"
#include "parser/table_ref.h"

namespace peloton {
namespace parser {

class VariableSetStatement : public SQLStatement {
 public:
  VariableSetStatement() : SQLStatement(StatementType::VARIABLE_SET) {};
  virtual ~VariableSetStatement() {};

  virtual void Accept(UNUSED_ATTRIBUTE SqlNodeVisitor* v) const override {};
};
} // namespace parser
} // namespace peloton
