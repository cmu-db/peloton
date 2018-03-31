//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// explain_statement.h
//
// Identification: src/include/parser/explain_statement.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/sql_statement.h"

namespace peloton {
namespace parser {

/**
 * @class ExplainStatement
 * @brief Represents "EXPLAIN <query>"
 */
class ExplainStatement : public SQLStatement {
 public:
  ExplainStatement() : SQLStatement(StatementType::EXPLAIN) {}
  virtual ~ExplainStatement() {}

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  std::unique_ptr<parser::SQLStatement> real_sql_stmt;
};

}  // namespace parser
}  // namespace peloton
