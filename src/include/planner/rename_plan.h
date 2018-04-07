//
// Created by Nevermore on 03/04/2018.
//

#pragma once

#include "planner/abstract_plan.h"
#include "parser/rename_function_statement.h"

namespace peloton {
namespace planner {
class RenamePlan : public AbstractPlan {
 public:
  explicit RenamePlan() = delete;

  explicit RenamePlan(parser::RenameFuncStatement *tree)
      : AbstractPlan(),
        obj_type(tree->type),
        table_name_(tree->table_info_->table_name),
        db_name_(tree->GetDatabaseName()) {
    old_names_.emplace_back(std::string{tree->oldName});
    new_names_.emplace_back(std::string{tree->newName});
    LOG_TRACE("Build rename plan table: %s, db: %s, oldname: %s, new name: %s",
              const_cast<char *>(table_name_.c_str()),
              const_cast<char *>(db_name_.c_str()),
              const_cast<char *>(old_names_[0].c_str()),
              const_cast<char *>(new_names_[0].c_str()));
  }

  RenamePlan(parser::RenameFuncStatement::ObjectType obj_t, std::string tb_name,
             std::string db_name, std::vector<std::string> old_names,
             std::vector<std::string> new_names)
      : AbstractPlan(),
        old_names_(old_names),
        new_names_(new_names),
        obj_type(obj_t),
        table_name_(tb_name),
        db_name_(db_name) {}

  virtual ~RenamePlan() {}

  virtual PlanNodeType GetPlanNodeType() const { return PlanNodeType::RENAME; }

  const std::string GetInfo() const override {
    return StringUtil::Format(
        "Rename Object %d, old name %s, new name %s, table %s, db %s\n",
        this->obj_type, const_cast<char *>(this->old_names_[0].c_str()),
        const_cast<char *>(this->new_names_[0].c_str()),
        const_cast<char *>(this->table_name_.c_str()),
        const_cast<char *>(this->db_name_.c_str()));
  }

  std::unique_ptr<AbstractPlan> Copy() const {
    return std::unique_ptr<AbstractPlan>(
        new RenamePlan(this->obj_type, this->table_name_, this->db_name_,
                       this->old_names_, this->new_names_));
  }

  std::string GetOldName() const { return this->old_names_[0]; }

  std::string GetNewName() const { return this->new_names_[0]; }

  std::string GetDatabaseName() const { return this->db_name_; }

  std::string GetTableName() const { return this->table_name_; }

  parser::RenameFuncStatement::ObjectType GetObjectType() {
    return this->obj_type;
  }

 private:
  // those names are corresponding with their indexes
  std::vector<std::string> old_names_;
  std::vector<std::string> new_names_;

  parser::RenameFuncStatement::ObjectType obj_type;

  std::string table_name_;
  std::string db_name_;
};
}
}
