//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// explain_statement.h
//
// Identification: src/include/parser/explain_statement.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/logger.h"
#include "common/sql_node_visitor.h"
#include "expression/abstract_expression.h"
#include "parser/sql_statement.h"
#include "parser/table_ref.h"

namespace peloton {
namespace parser {

/**
 * @class ExplainStatement
 * @brief Represents "EXPLAIN <query>"
 */
class ExplainStatement : public SQLStatement {
 public:
  ExplainStatement()
      : SQLStatement(StatementType::EXPLAIN) {}
  virtual ~ExplainStatement() {}

  virtual void Accept(SqlNodeVisitor* /*v*/) override { }

  std::unique_ptr<parser::SQLStatement> real_sql_stmt;
};

}  // namespace parser
}  // namespace peloton
