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
      : SQLStatement(StatementType::INSERT), type(type), select(nullptr) {}

  virtual ~InsertStatement() {}

  virtual void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  inline std::string GetTableName() const { return table_ref_->GetTableName(); }

  inline void TryBindDatabaseName(std::string default_database_name) {
    table_ref_->TryBindDatabaseName(default_database_name);
  }

  inline std::string GetDatabaseName() const {
    return table_ref_->GetDatabaseName();
  }

  const std::string GetInfo(int num_indent) const override;

  const std::string GetInfo() const override;

  InsertType type;
  std::vector<std::string> columns;
  std::vector<std::vector<std::unique_ptr<expression::AbstractExpression>>>
      insert_values;
  std::unique_ptr<SelectStatement> select;

  // Which table are we inserting into
  std::unique_ptr<TableRef> table_ref_;
};

}  // namespace parser
}  // namespace peloton
