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
        type_(type),
        missing_(false),
        cascade_(false) {}

  DropStatement(EntityType type, std::string table_name_of_trigger,
                std::string trigger_name)
      : TableRefStatement(StatementType::DROP),
        type_(type),
        table_name_of_trigger_(table_name_of_trigger),
        trigger_name_(trigger_name) {}

  EntityType GetDropType() { return type_; }

  bool GetMissing() { return missing_; }

  void SetMissing(bool missing) { missing_ = missing; }

  bool GetCascade() { return cascade_; }

  void SetCascade(bool cascade) { cascade_ = cascade; }

  std::string &GetIndexName() { return index_name_; }

  void SetIndexName(std::string &index_name) { index_name_ = index_name; }

  void SetIndexName(char *index_name) { index_name_ = index_name; }

  std::string &GetPrepStmt() { return prep_stmt_; }

  void SetPrepStmt(std::string &prep_stmt) { prep_stmt_ = prep_stmt; }

  void SetPrepStmt(char *prep_stmt) { prep_stmt_ = prep_stmt; }

  std::string &GetSchemaName() { return schema_name_; }

  void SetSchemaName(std::string &schema_name) { schema_name_ = schema_name; }

  void SetSchemaName(char *schema_name) { schema_name_ = schema_name; }

  std::string &GetTriggerName() { return trigger_name_; }

  void SetTriggerName(std::string &trigger_name) {
    trigger_name_ = trigger_name;
  }

  void SetTriggerName(char *trigger_name) { trigger_name_ = trigger_name; }

  std::string &GetTriggerTableName() { return table_name_of_trigger_; }

  void SetTriggerTableName(std::string &table_name_of_trigger) {
    table_name_of_trigger_ = table_name_of_trigger;
  }

  void SetTriggerTableName(char *table_name_of_trigger) {
    table_name_of_trigger_ = table_name_of_trigger;
  }

  virtual ~DropStatement() {}

  virtual void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  const std::string GetInfo(int num_indent) const override {
    std::ostringstream os;
    os << StringUtil::Indent(num_indent) << "DropStatement\n";
    os << StringUtil::Indent(num_indent + 1);
    switch (type_) {
      case kDatabase: {
        os << "DropType: Database\n";
        os << StringUtil::Indent(num_indent + 1)
           << "Database name: " << GetDatabaseName() << "\n";
        break;
      }
      case kTable: {
        os << "DropType: Table\n";
        os << StringUtil::Indent(num_indent + 1)
           << "Table name: " << GetTableName() << "\n";
        break;
      }
      case kSchema: {
        os << "DropType: Schema\n";
        os << StringUtil::Indent(num_indent + 1)
           << "Schema name: " << schema_name_ << "\n";
        break;
      }
      case kIndex: {
        os << "DropType: Index\n";
        os << StringUtil::Indent(num_indent + 1)
           << "Index name: " << index_name_ << "\n";
        break;
      }
      case kView: {
        os << "DropType: View\n";
        os << StringUtil::Indent(num_indent + 1)
           << "Drop view is not implemented yet...\n";
        break;
      }
      case kPreparedStatement: {
        os << "DropType: PreparedStatement\n";
        os << StringUtil::Indent(num_indent + 1)
           << "Prepared statement: " << prep_stmt_ << "\n";
        break;
      }
      case kTrigger: {
        os << "DropType: Trigger\n";
        os << StringUtil::Indent(num_indent + 1)
           << "Trigger table name: " << table_name_of_trigger_ << "\n";
        os << StringUtil::Indent(num_indent + 1)
           << "Trigger name: " << trigger_name_ << "\n";
        break;
      }
    }
    os << StringUtil::Indent(num_indent + 1)
       << "IF EXISTS: " << ((missing_) ? "True\n" : "False\n");
    os << StringUtil::Indent(num_indent + 1)
       << "CASCADE: " << ((cascade_) ? "True\n" : "False\n");
    os << StringUtil::Indent(num_indent + 1)
       << "RESTRICT: " << ((cascade_) ? "False\n" : "True\n");
    return os.str();
  }

  const std::string GetInfo() const override {
    std::ostringstream os;

    os << "SQLStatement[DROP]\n";

    os << GetInfo(1);

    return os.str();
  }

 private:
  // Type of DROP
  EntityType type_;
  // IF EXISTS
  bool missing_;
  // CASCADE or RESTRICT
  bool cascade_;

  // drop index
  std::string index_name_;

  // drop prepared statement
  std::string prep_stmt_;

  // drop trigger
  std::string table_name_of_trigger_;
  std::string trigger_name_;

  // drop schema
  std::string schema_name_;
};

}  // namespace parser
}  // namespace peloton
