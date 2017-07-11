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
      : SQLStatement(StatementType::ANALYZE),
        analyze_table(nullptr),
        analyze_columns(nullptr) {};

  // TODO: use smart pointer to avoid raw delete.
  virtual ~AnalyzeStatement() {
    if (analyze_columns != nullptr) {
      for (auto col : *analyze_columns) delete[] col;
      delete analyze_columns;
    }
    if (analyze_table != nullptr) {
      delete analyze_table;
    }
  }

  std::string GetTableName() const {
    if (analyze_table == nullptr) {
      return INVALID_NAME;
    }
    return analyze_table->GetTableName();
  }

  std::vector<char*> GetColumnNames() const {
    if (analyze_columns == nullptr) {
      return {};
    }
    return (*analyze_columns);
  }

  std::string GetDatabaseName() const {
    if (analyze_table == nullptr) {
      return INVALID_NAME;
    }
    return analyze_table->GetDatabaseName();
  }

  virtual void Accept(SqlNodeVisitor* v) const override {
    v->Visit(this);
  }

  parser::TableRef* analyze_table;
  std::vector<char*>* analyze_columns;

  const std::string INVALID_NAME = "";
};

}  // End parser namespace
}  // End peloton namespace
