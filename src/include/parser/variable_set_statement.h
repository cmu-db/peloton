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
/* TODO(Yuchen): Do not support VariableSetStatement yet
 * When JDBC starts connection, it will send SET statement and need server's
 * response to build the connection.
 * Add VariableSetStatement here so it can be handled by the parser to avoid
 * connection error.
 * Actually after parsing stage, the statement will not be processed by the
 * server.
 * It will be skipped. See HardcodedExecuteFilter()
 */
class VariableSetStatement : public SQLStatement {
 public:
  VariableSetStatement() : SQLStatement(StatementType::VARIABLE_SET){};
  virtual ~VariableSetStatement() {}

  virtual void Accept(UNUSED_ATTRIBUTE SqlNodeVisitor *v) override {}
};
}  // namespace parser
}  // namespace peloton
