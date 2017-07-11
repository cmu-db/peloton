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
  char* column;
  expression::AbstractExpression* value;

  ~UpdateClause() {
    if (column != nullptr) {
      delete[] column;
    }
    if (value != nullptr) {
      delete value;
    }
  }

  UpdateClause* Copy() {
    UpdateClause* new_clause = new UpdateClause();
    std::string str(column);
    char* new_cstr = new char[str.length() + 1];
    std::strcpy(new_cstr, str.c_str());
    expression::AbstractExpression* new_expr = value->Copy();
    new_clause->column = new_cstr;
    new_clause->value = new_expr;
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
      : SQLStatement(StatementType::UPDATE),
        table(NULL),
        updates(NULL),
        where(NULL) {}

  virtual ~UpdateStatement() {
    if (table != nullptr) {
      delete table;
    }

    if (updates != nullptr) {
      for (auto clause : *updates) {
        if (clause != nullptr) {
          delete clause;
        }
      }
      delete updates;
    }

    if (where != nullptr) {
      delete where;
    }
  }

  virtual void Accept(SqlNodeVisitor* v) const override {
    v->Visit(this);
  }

  // TODO: switch to char* instead of TableRef
  TableRef* table;
  std::vector<UpdateClause*>* updates;
  expression::AbstractExpression* where = nullptr;
};

}  // End parser namespace
}  // End peloton namespace
