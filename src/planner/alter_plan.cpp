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

AlterPlan::AlterPlan(const std::string &database_name,
                               const std::string &table_name,
                               //std::unique_ptr<catalog::Schema> added_columns,
                               const std::vector<std::string> &dropped_columns,
                               AlterType a_type)
    : table_name(table_name),
      database_name(database_name),
      //added_columns(added_columns.release()),
      dropped_columns(dropped_columns),
      type(a_type) {}

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
  case parser::AlterTableStatement::AlterTableType::DROP:
    for (auto col : *parse_tree->names) {
      LOG_TRACE("Drooped column name: %s", col);
      dropped_columns.push_back(std::string(col));
      type = AlterType::DROP;
    }
    break;
  case parser::AlterTableStatement::AlterTableType::ADD:
  default:
    LOG_ERROR("Not Implemented the plan yet!");
    type = AlterType::INVALID;
  }
    //std::vector<catalog::Column> columns;
    // case 1: add column(column name + column data type)
    //if (parse_tree->type == AlterTableType::COLUMN) {
    // Add columns: traverse through vector of ColumnDefinition
    /*for (auto col : *parse_tree->columns) {
      type::TypeId val = col->GetValueType(col->type);
      LOG_TRACE("Column name: %s", col->name);

      bool is_inline = (val == type::TypeId::VARCHAR) ? false : true;
      auto column = catalog::Column(val, type::Type::GetTypeSize(val),
                                    std::string(col->name), is_inline);
      LOG_TRACE("Column is_line: %d", is_inline);
      // Add not_null constraints
      if (col->not_null) {
        catalog::Constraint constraint(ConstraintType::NOTNULL, "con_not_null");
        column.AddConstraint(constraint);
      }
      columns.push_back(column);
    }
    added_columns = new catalog::Schema(columns);*/

    // Drop columns: traverse through vector of char*(column name)

  }






}  // namespace planner
}  // namespace peloton
