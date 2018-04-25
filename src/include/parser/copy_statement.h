//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// copy_statement.h
//
// Identification: src/include/parser/copy_statement.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/select_statement.h"
#include "parser/sql_statement.h"
#include "parser/table_ref.h"
#include "common/sql_node_visitor.h"

namespace peloton {
namespace parser {

/**
 * @class CopyStatement
 * @brief Represents PSQL Copy statements.
 */
class CopyStatement : public SQLStatement {
 public:
  CopyStatement()
      : SQLStatement(StatementType::COPY),
        table(nullptr),
        type(),
        delimiter(',') {}

  ~CopyStatement() = default;

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  const std::string GetInfo(int num_indent) const override;

  const std::string GetInfo() const override;

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Public member fields
  ///
  //////////////////////////////////////////////////////////////////////////////

  std::unique_ptr<TableRef> table;

  std::unique_ptr<SelectStatement> select_stmt;

  CopyType type;

  std::string file_path;

  ExternalFileFormat format;

  bool is_from;

  char delimiter;
};

}  // namespace parser
}  // namespace peloton
