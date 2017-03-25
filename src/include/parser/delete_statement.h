//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// delete_statement.h
//
// Identification: src/include/parser/delete_statement.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/logger.h"
#include "common/sql_node_visitor.h"
#include "parser/sql_statement.h"

namespace peloton {
namespace parser {

/**
 * @struct DeleteStatement
 * @brief Represents "DELETE FROM students WHERE grade > 3.0"
 *
 * If expr == NULL => delete all rows (truncate)
 */
struct DeleteStatement : SQLStatement {
  DeleteStatement()
      : SQLStatement(StatementType::DELETE),
        table_ref(nullptr), expr(nullptr) {};

  virtual ~DeleteStatement() {
    // FIXME The following line should be removed
    //       when the old parser gets obsolete
    delete table_info_;

    delete table_ref;
    delete expr;
  }

  std::string GetTableName() const {
    // FIXME The following two lines should be removed
    //       when the old parser gets obsolete
    if (table_info_ != nullptr)
      return table_info_->table_name;

    return table_ref->GetTableName();
  }

  std::string GetDatabaseName() const {
    // FIXME The following four lines should be removed
    //       when the old parser gets obsolete
    if (table_info_ != nullptr) {
      if (table_info_->database_name == nullptr)
        return DEFAULT_DB_NAME;
      return table_info_->database_name;
    }

    return table_ref->GetDatabaseName();
  }

  virtual void Accept(SqlNodeVisitor* v) const override {
    v->Visit(this);
  }

  // FIXME The following line should be removed
  //       when the old parser gets obsolete
  parser::TableInfo* table_info_ = nullptr;

  parser::TableRef* table_ref;
  expression::AbstractExpression* expr;
};

}  // End parser namespace
}  // End peloton namespace
