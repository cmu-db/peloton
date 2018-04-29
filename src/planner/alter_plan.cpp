//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// alter_table_plan.cpp
//
// Identification: src/planner/alter_table_plan.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/alter_plan.h"

#include "catalog/column.h"
#include "catalog/schema.h"
#include "parser/alter_statement.h"
#include "storage/data_table.h"

namespace peloton {
namespace planner {

AlterPlan::AlterPlan(
    const std::string &database_name, const std::string &table_name,
    const std::vector<std::string> &dropped_columns,
    UNUSED_ATTRIBUTE const std::vector<std::unique_ptr<catalog::Schema>>
        &added_columns_,
    AlterType a_type)
    : table_name(table_name),
      database_name(database_name),
      dropped_columns(dropped_columns),
      type(a_type) {
  //        for (size_t i=0;i<added_columns.size();i++){
  //          this->added_columns.push_back(std::move(added_columns_[i]));
  //        }
}

AlterPlan::AlterPlan(const std::string &database_name,
                     const std::string &table_name,
                     const std::vector<std::string> &old_names,
                     const std::vector<std::string> &new_names,
                     const AlterType a_type)
    : table_name(table_name),
      database_name(database_name),
      old_names_(old_names),
      new_names_(new_names),
      type(a_type) {}

AlterPlan::AlterPlan(parser::AlterTableStatement *parse_tree) {
  table_name = std::string(parse_tree->GetTableName());
  database_name = std::string(parse_tree->GetDatabaseName());
  switch (parse_tree->type) {
    case parser::AlterTableStatement::AlterTableType::RENAME: {
      old_names_.emplace_back(std::string{parse_tree->oldName});
      new_names_.emplace_back(std::string{parse_tree->newName});
      type = AlterType::RENAME;
      break;
    }
    case parser::AlterTableStatement::AlterTableType::ALTER: {
      // deal with dropped columns
      for (auto col : *parse_tree->dropped_names) {
        LOG_TRACE("Drooped column name: %s", col);
        dropped_columns.push_back(std::string(col));
        type = AlterType::ALTER;
      }

      // deal with added columns
      std::vector<catalog::Column> columns;
      for (size_t i = 0; i < (*parse_tree->added_columns).size(); i++) {
        type::TypeId val = parser::ColumnDefinition::GetValueType(
            (*parse_tree->added_columns)[i].get()->type);
        // LOG_TRACE("Column Name: %s", (char
        // *)((*parse_tree->columns)[i].get()->name.c_str()));

        bool is_inline = (val == type::TypeId::VARCHAR) ? false : true;
        auto column = catalog::Column(
            val, type::Type::GetTypeSize(val),
            std::string((*parse_tree->added_columns)[i].get()->name),
            is_inline);

        if ((*parse_tree->added_columns)[i].get()->not_null) {
          catalog::Constraint constraint(ConstraintType::NOTNULL,
                                         "con_not_null");
          column.AddConstraint(constraint);
        }
        columns.emplace_back(column);
      }
      added_columns.emplace_back(
          std::unique_ptr<catalog::Schema>(new catalog::Schema(columns)));

      // deal with change column types
      for (size_t i = 0; i < (*parse_tree->changed_type_columns).size(); i++) {
        auto &tmp = (*parse_tree->changed_type_columns)[i];
        type::TypeId val =
            parser::ColumnDefinition::GetValueType(tmp.get()->type);
        std::string name = tmp.get()->name;
        changed_type_columns.emplace_back(std::make_pair(name, val));
      }
    }
    default:
      LOG_ERROR("Not Implemented the plan yet!");
      type = AlterType::INVALID;
  }
}

}  // namespace planner
}  // namespace peloton
