//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// alter_plan.h
//
// Identification: src/include/parser/alter_plan.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include "planner/abstract_plan.h"
#include "parser/alter_statement.h"
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
 *  TODO: adding support for add/drop column
 */
class AlterPlan : public AbstractPlan {
 public:
  AlterPlan() = delete;

  explicit AlterPlan(const std::string &database_name,
                          const std::string &table_name,
                          //std::unique_ptr<catalog::Schema> added_columns,
                          const std::vector<std::string> &dropped_columns,
                          AlterType a_type);
  explicit AlterPlan(const std::string &database_name,
                     const std::string &table_name,
                     const std::vector<std::string> &old_names,
                     const std::vector<std::string> &new_names, AlterType a_type);
  explicit AlterPlan(parser::AlterTableStatement *parse_tree);

  virtual ~AlterPlan() {}

  virtual PlanNodeType GetPlanNodeType() const { return PlanNodeType::ALTER; }

  virtual std::unique_ptr<AbstractPlan> Copy() { return nullptr; }

  const std::string GetInfo() const {
    return StringUtil::Format("AlterPlan table:%s, database:%s",
                              this->table_name.c_str(), this->database_name.c_str());
  }

  std::unique_ptr<AbstractPlan> Copy() const {
    switch(this->type) {
    case AlterType::DROP:
    case AlterType::ADD:
      return std::unique_ptr<AbstractPlan>(
          new AlterPlan(database_name, table_name,
              //std::unique_ptr<catalog::Schema>(
              //    catalog::Schema::CopySchema(added_columns)),
                        dropped_columns, type));
    case AlterType::RENAME:
      return std::unique_ptr<AbstractPlan>(
          new AlterPlan(database_name, table_name, old_names_, new_names_, type));
    default:
      LOG_ERROR("Not supported Copy of Alter type yet");
      return nullptr;
    }
  }

  std::string GetTableName() const { return table_name; }

  std::string GetDatabaseName() const { return database_name; }

  //catalog::Schema *GetAddedColumns() const { return added_columns; }

  const std::vector<std::string> &GetDroppedColumns() const {
    return dropped_columns;
  }

  AlterType GetAlterTableType() const { return type; }

  //function used for rename statement
  std::string GetOldName() const { return this->old_names_[0]; }

  //function used for rename statement
  std::string GetNewName() const { return this->new_names_[0]; }

  //return true if the alter plan is rename statement
  bool IsRename() const { return this->type==AlterType::RENAME;}
private:
// Target Table
  storage::DataTable *target_table_ = nullptr;

  // Table Name
  std::string table_name;

  // Database Name
  std::string database_name;

  // Schema delta, define the column txn want to add
  // catalog::Schema *added_columns;
  // dropped_column, define the column you want to drop
  std::vector<std::string> dropped_columns;

  //used for store rename function data
  std::vector<std::string> old_names_;
  std::vector<std::string> new_names_;

  // Check to either AlterTable Table, INDEX or Rename
  AlterType type;

};
}
}
