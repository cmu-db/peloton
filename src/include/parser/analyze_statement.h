//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// analyze_statement.h
//
// Identification: src/include/parser/analyze_statement.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "common/logger.h"
#include "common/sql_node_visitor.h"
#include "parser/sql_statement.h"
#include "parser/table_ref.h"

namespace peloton {
namespace parser {

class AnalyzeStatement : public SQLStatement {
 public:
  AnalyzeStatement()
      : SQLStatement(StatementType::ANALYZE), analyze_table(nullptr){};

  virtual ~AnalyzeStatement() {}

  std::string GetTableName() const {
    if (analyze_table == nullptr) {
      return INVALID_NAME;
    }
    return analyze_table->GetTableName();
  }

  const std::vector<std::string> &GetColumnNames() const {
    return analyze_columns;
  }

  inline void TryBindDatabaseName(std::string default_database_name) {
    if (analyze_table != nullptr)
      analyze_table->TryBindDatabaseName(default_database_name);
  }

  std::string GetDatabaseName() const {
    if (analyze_table == nullptr) {
      return INVALID_NAME;
    }
    return analyze_table->GetDatabaseName();
  }

  virtual void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  const std::string GetInfo(int num_indent) const override;

  const std::string GetInfo() const override;

  std::unique_ptr<parser::TableRef> analyze_table;
  std::vector<std::string> analyze_columns;

  const std::string INVALID_NAME = "";
};

}  // namespace parser
}  // namespace peloton
