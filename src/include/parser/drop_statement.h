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
      : TableRefStatement(StatementType::DROP),
        type(type),
        missing(false),
        cascade(false) {}

  DropStatement(EntityType type, std::string table_name_of_trigger,
                std::string trigger_name)
      : TableRefStatement(StatementType::DROP),
        type(type),
        table_name_of_trigger(table_name_of_trigger),
        trigger_name(trigger_name) {}

  EntityType GetDropType() { return this->type; }

  bool GetMissing() { return this->missing; }

  std::string GetIndexName() { return this->index_name; }

  std::string GetPrepStmt() { return this->prep_stmt; }

  std::string GetSchemaName() { return this->schema_name; }

  std::string GetTriggerName() { return this->trigger_name; }

  std::string GetTriggerTableName() { return this->table_name_of_trigger; }

  virtual ~DropStatement() {}

  virtual void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  // Type of DROP
  EntityType type;
  // CASCADE or RESTRICT
  bool cascade;
  // IF EXISTS
  bool missing;

  // drop index
  std::string index_name;
  std::string prep_stmt;

  // drop trigger
  std::string table_name_of_trigger;
  std::string trigger_name;

  // drop schema
  std::string schema_name;
};

}  // namespace parser
}  // namespace peloton
