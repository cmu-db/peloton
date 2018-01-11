//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// statement_import.h
//
// Identification: src/include/parser/statement_import.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/sql_statement.h"
#include "parser/table_ref.h"
#include "expression/constant_value_expression.h"
#include "common/sql_node_visitor.h"

namespace peloton {
namespace parser {

/**
 * @class CopyStatement
 * @brief Represents PSQL Copy statements.
 */
class CopyStatement : public SQLStatement {
 public:
  CopyStatement(CopyType type)
      : SQLStatement(StatementType::COPY),
        cpy_table(nullptr),
        type(type),
        delimiter(','){};

  virtual ~CopyStatement() {}

  virtual void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  const std::string GetInfo(int num_indent) const override;

  const std::string GetInfo() const override;

  std::unique_ptr<TableRef> cpy_table;

  CopyType type;

  std::string file_path;
  char delimiter;
};

}  // namespace parser
}  // namespace peloton
