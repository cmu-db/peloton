//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// statement_insert.h
//
// Identification: src/include/parser/statement_insert.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/sql_node_visitor.h"
#include "parser/sql_statement.h"
#include "select_statement.h"

namespace peloton {
namespace parser {

/**
 * @class InsertStatement
 * @brief Represents "INSERT INTO students VALUES ('Max', 1112233,
 * 'Musterhausen', 2.3)"
 */
class InsertStatement : SQLStatement {
 public:
  InsertStatement(InsertType type)
      : SQLStatement(StatementType::INSERT),
        type(type),
        columns(NULL),
        insert_values(NULL),
        select(NULL) {}

  virtual ~InsertStatement() {
    if (columns != nullptr) {
      for (auto col : *columns) {
        if (col != nullptr) {
          delete[] col;
        }
      }
      delete columns;
    }

    if (insert_values != nullptr) {
      for (auto *tuple : *insert_values) {
        for (auto *expr : *tuple) {
          // Why?
//          if (expr->GetExpressionType() != ExpressionType::VALUE_PARAMETER)
            if (expr != nullptr) {
              delete expr;
            }
        }
        delete tuple;
      }
      delete insert_values;
    }

    if (select != nullptr) {
      delete select;
    }

    if (table_ref_ != nullptr) {
      delete table_ref_;
    }
  }

  virtual void Accept(SqlNodeVisitor* v) const override { v->Visit(this); }

  inline std::string GetTableName() const {
    return table_ref_->GetTableName();
  }
  inline std::string GetDatabaseName() const {
    return table_ref_->GetDatabaseName();
  }

  InsertType type;
  std::vector<char*>* columns;
  std::vector<std::vector<peloton::expression::AbstractExpression*>*>*
      insert_values;
  SelectStatement* select;

  // Which table are we inserting into
  TableRef* table_ref_;
};

}  // End parser namespace
}  // End peloton namespace
