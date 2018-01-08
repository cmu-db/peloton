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
#include "expression/abstract_expression.h"
#include "parser/sql_statement.h"
#include "parser/table_ref.h"

namespace peloton {
namespace parser {

/**
 * @class DeleteStatement
 * @brief Represents "DELETE FROM students WHERE grade > 3.0"
 *
 * If expr == NULL => delete all rows (truncate)
 */
class DeleteStatement : public SQLStatement {
 public:
  DeleteStatement()
      : SQLStatement(StatementType::DELETE),
        table_ref(nullptr),
        expr(nullptr){};

  virtual ~DeleteStatement() {}

  std::string GetTableName() const { return table_ref->GetTableName(); }

  inline void TryBindDatabaseName(std::string default_database_name) {
    if (table_ref != nullptr)
      table_ref->TryBindDatabaseName(default_database_name);
  }

  std::string GetDatabaseName() const { return table_ref->GetDatabaseName(); }

  virtual void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  const std::string GetInfo(int num_indent) const override;

  const std::string GetInfo() const override;

  std::unique_ptr<parser::TableRef> table_ref;
  std::unique_ptr<expression::AbstractExpression> expr;
};

}  // namespace parser
}  // namespace peloton
