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

class SqlNodeVisitor;

namespace parser {

/**
 * @class ExplainStatement
 * @brief Represents "EXPLAIN <query>"
 */
class ExplainStatement : public SQLStatement {
 public:
  ExplainStatement() : SQLStatement(StatementType::EXPLAIN) {}
  virtual ~ExplainStatement() {}

  virtual void Accept(UNUSED_ATTRIBUTE SqlNodeVisitor *v) override {}

  std::unique_ptr<parser::SQLStatement> real_sql_stmt;
};

}  // namespace parser
}  // namespace peloton
