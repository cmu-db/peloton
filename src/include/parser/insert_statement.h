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
 * @struct InsertStatement
 * @brief Represents "INSERT INTO students VALUES ('Max', 1112233,
 * 'Musterhausen', 2.3)"
 */
struct InsertStatement : SQLStatement {
  InsertStatement(InsertType type)
      : SQLStatement(StatementType::INSERT),
        type(type),
        columns(NULL),
        insert_values(NULL),
        select(NULL),
        table_ref_(nullptr) {}

  virtual ~InsertStatement() {
    if (columns) {
      for (auto col : *columns) free(col);
      delete columns;
    }

    if (insert_values) {
      for (auto tuple : *insert_values) {
        for (auto expr : *tuple) {
          if (expr->GetExpressionType() != ExpressionType::VALUE_PARAMETER)
            delete expr;
        }
        delete tuple;
      }
      delete insert_values;
    }

    delete select;
  }

  virtual void Accept(SqlNodeVisitor* v) const override { v->Visit(this); }

  inline std::string GetTableName() const { return table_ref_->GetTableName(); }
  inline std::string GetDatabaseName() const {
    return table_ref_->GetDatabaseName();
  }

  InsertType type;
  std::vector<char*>* columns;
  std::vector<std::vector<peloton::expression::AbstractExpression*>*>*
      insert_values;
  SelectStatement* select;

  // FIXME: This is here for compilation purpose. Need to remove after the
  // Hyrise parser is removed!!!
  TableInfo* table_info_ = nullptr;

  // Which table are we inserting into
  TableRef* table_ref_;
};

}  // End parser namespace
}  // End peloton namespace
