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
  std::unique_ptr<char[]> column;
  std::unique_ptr<expression::AbstractExpression> value;

  ~UpdateClause() {}

  UpdateClause* Copy() {
    UpdateClause* new_clause = new UpdateClause();
    std::string str(column.get());
    char* new_cstr = new char[str.length() + 1];
    std::strcpy(new_cstr, str.c_str());
    expression::AbstractExpression* new_expr = value->Copy();
    new_clause->column.reset(new_cstr);
    new_clause->value.reset(new_expr);
    return new_clause;
  }
};

/**
 * @struct UpdateStatement
 * @brief Represents "UPDATE"
 */
struct UpdateStatement : SQLStatement {
  UpdateStatement()
      : SQLStatement(StatementType::UPDATE),
        table(nullptr),
        updates(nullptr),
        where(nullptr) {}

  virtual ~UpdateStatement() {}

  virtual void Accept(SqlNodeVisitor* v) const override {
    v->Visit(this);
  }

  // TODO: switch to char* instead of TableRef
  std::unique_ptr<TableRef> table;
  std::unique_ptr<std::vector<std::unique_ptr<UpdateClause>>> updates;
  std::unique_ptr<expression::AbstractExpression> where = nullptr;
};

}  // End parser namespace
}  // End peloton namespace
