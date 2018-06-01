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

  const std::string GetInfo(int num_indent) const override;

  const std::string GetInfo() const override;

  std::unique_ptr<parser::SQLStatement> real_sql_stmt;

  /**
   * @brief Should be set by the binder, used in the executor to bind the stmt
   * being explained
   */
  std::string default_database_name;
};

}  // namespace parser
}  // namespace peloton
