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

  // The table that is copied into or copied from
  std::unique_ptr<TableRef> table;

  // The SQL statement used instead of a table when copying data out to a file
  std::unique_ptr<SelectStatement> select_stmt;

  // The set of attributes being written out or read in
  std::vector<std::unique_ptr<expression::AbstractExpression>> select_list;

  // The type of copy
  CopyType type;

  // The input or output file that is read of written into
  std::string file_path;

  // The format of the file
  ExternalFileFormat format = ExternalFileFormat::CSV;

  bool is_from;

  char delimiter = ',';
  char quote = '"';
  char escape = '"';
};

}  // namespace parser
}  // namespace peloton
