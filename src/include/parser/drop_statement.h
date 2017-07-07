//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// statement_drop.h
//
// Identification: src/include/parser/statement_drop.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/sql_statement.h"
#include "common/sql_node_visitor.h"

namespace peloton {
namespace parser {

/**
 * @class DropStatement
 * @brief Represents "DROP TABLE"
 */
class DropStatement : public TableRefStatement {
 public:
  enum EntityType {
    kDatabase,
    kTable,
    kSchema,
    kIndex,
    kView,
    kPreparedStatement,
    kTrigger
  };

  // helper for c_str copy
  static char* cstrdup(const char* c_str) {
    char* new_str = new char[strlen(c_str) + 1];
    strcpy(new_str, c_str);
    return new_str;
  }

  DropStatement(EntityType type)
      : TableRefStatement(StatementType::DROP), type(type), missing(false) {}

  DropStatement(EntityType type, std::string table_name_of_trigger, std::string trigger_name)
      : TableRefStatement(StatementType::DROP),
        type(type),
        table_name_of_trigger(cstrdup(table_name_of_trigger.c_str())),
        trigger_name(cstrdup(trigger_name.c_str())) {}

  virtual ~DropStatement() {}

  virtual void Accept(SqlNodeVisitor* v) const override {
    v->Visit(this);
  }

  EntityType type;
  std::unique_ptr<char[]> database_name = nullptr;
  std::unique_ptr<char[]> index_name = nullptr;
  std::unique_ptr<char[]> prep_stmt = nullptr;
  bool missing;

  // drop trigger
  char* table_name_of_trigger = nullptr;
  char* trigger_name = nullptr;
};

}  // namespace parser
}  // namespace peloton
