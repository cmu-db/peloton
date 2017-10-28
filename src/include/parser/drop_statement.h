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

  DropStatement(EntityType type)
      : TableRefStatement(StatementType::DROP), type(type), missing(false) {}

  DropStatement(EntityType type, std::string table_name_of_trigger, std::string trigger_name)
      : TableRefStatement(StatementType::DROP),
        type(type),
        table_name_of_trigger(table_name_of_trigger),
        trigger_name(trigger_name) {}

  virtual ~DropStatement() {}

  virtual void Accept(SqlNodeVisitor* v) override {
    v->Visit(this);
  }

  EntityType type;
  std::string database_name;
  std::string index_name;
  std::string prep_stmt;
  bool missing;

  // drop trigger
  std::string table_name_of_trigger;
  std::string trigger_name;
};

}  // namespace parser
}  // namespace peloton
