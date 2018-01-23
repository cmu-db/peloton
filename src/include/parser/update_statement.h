//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// statement_update.h
//
// Identification: src/include/parser/statement_update.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstring>

#include "expression/abstract_expression.h"
#include "common/sql_node_visitor.h"
#include "parser/sql_statement.h"
#include "parser/table_ref.h"

namespace peloton {
namespace parser {

/**
 * @struct UpdateClause
 * @brief Represents "column = value" expressions
 */
class UpdateClause {
 public:
  std::string column;
  std::unique_ptr<expression::AbstractExpression> value;

  ~UpdateClause() {}

  UpdateClause *Copy() {
    UpdateClause *new_clause = new UpdateClause();
    std::string str(column);
    new_clause->column = column;
    expression::AbstractExpression *new_expr = value->Copy();
    new_clause->value.reset(new_expr);
    return new_clause;
  }
};

/**
 * @class UpdateStatement
 * @brief Represents "UPDATE"
 */
class UpdateStatement : public SQLStatement {
 public:
  UpdateStatement()
      : SQLStatement(StatementType::UPDATE), table(nullptr), where(nullptr) {}

  virtual ~UpdateStatement() {}

  virtual void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  const std::string GetInfo(int num_indent) const override;

  const std::string GetInfo() const override;

  // TODO: switch to char* instead of TableRef
  std::unique_ptr<TableRef> table;
  std::vector<std::unique_ptr<UpdateClause>> updates;
  std::unique_ptr<expression::AbstractExpression> where = nullptr;
};

}  // namespace parser
}  // namespace peloton
