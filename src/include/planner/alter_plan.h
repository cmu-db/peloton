//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// alter_plan.h
//
// Identification: src/include/parser/alter_plan.h
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/alter_statement.h"
#include "planner/abstract_plan.h"

namespace peloton {

namespace parser {
class AlterTableStatement;
}
namespace catalog {
class Schema;
}
namespace storage {
class DataTable;
}

namespace planner {

/** @brief The plan used for altering
 */
class AlterPlan : public AbstractPlan {
 public:
  AlterPlan() = delete;

  explicit AlterPlan(
      const std::string &database_name, const std::string &table_name,
      const std::vector<std::string> &dropped_columns,
      const std::unique_ptr<catalog::Schema> &added_columns,
      const std::unique_ptr<catalog::Schema> &changed_type_columns,
      AlterType a_type);

  explicit AlterPlan(const std::string &database_name,
                     const std::string &table_name, const std::string &old_name,
                     const std::string &new_name, AlterType a_type);

  explicit AlterPlan(parser::AlterTableStatement *parse_tree);

  virtual ~AlterPlan() {}

  virtual PlanNodeType GetPlanNodeType() const { return PlanNodeType::ALTER; }

  virtual std::unique_ptr<AbstractPlan> Copy() { return nullptr; }

  const std::string GetInfo() const {
    return StringUtil::Format("AlterPlan table:%s, database:%s",
                              this->table_name.c_str(),
                              this->database_name.c_str());
  }

  std::unique_ptr<AbstractPlan> Copy() const {
    switch (this->type) {
      case AlterType::ALTER:
        return std::unique_ptr<AbstractPlan>(
            new AlterPlan(database_name, table_name, dropped_columns,
                          added_columns, changed_type_columns, type));
      case AlterType::RENAME:
        return std::unique_ptr<AbstractPlan>(new AlterPlan(
            database_name, table_name, old_name_, new_name_, type));
      default:
        LOG_ERROR("Not supported Copy of Alter type yet");
        return nullptr;
    }
  }

  std::string GetTableName() const { return table_name; }

  std::string GetDatabaseName() const { return database_name; }

  const std::unique_ptr<catalog::Schema> &GetAddedColumns() const {
    return added_columns;
  }

  const std::vector<std::string> &GetDroppedColumns() const {
    return dropped_columns;
  }

  const std::unique_ptr<catalog::Schema> &GetChangedTypeColumns() const {
    return changed_type_columns;
  };

  AlterType GetAlterTableType() const { return type; }

  // function used for rename statement
  std::string GetOldName() const { return this->old_name_; }

  // function used for rename statement
  std::string GetNewName() const { return this->new_name_; }

  // return true if the alter plan is rename statement
  bool IsRename() const { return this->type == AlterType::RENAME; }

  // return schema name
  std::string GetSchemaName() const { return this->schema_name; }

 private:
  // Target Table
  storage::DataTable *target_table_ = nullptr;

  // Table Name
  std::string table_name;

  // Database Name
  std::string database_name;

  // Schema Name
  std::string schema_name;
  // Schema delta, define the column txn want to add
  std::unique_ptr<catalog::Schema> added_columns;
  // dropped_column, define the column you want to drop
  std::vector<std::string> dropped_columns;
  // changed-type columns, define the column you want to change type
  std::unique_ptr<catalog::Schema> changed_type_columns;
  // used for store rename function data
  std::string old_name_;
  std::string new_name_;

  // Check to either AlterTable Table, INDEX or Rename
  AlterType type;
};
}
}
