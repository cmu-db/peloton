//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// alter_plan.cpp
//
// Identification: src/planner/alter_plan.cpp
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
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
    const std::unique_ptr<catalog::Schema> &added_columns,
    const std::unique_ptr<catalog::Schema> &changed_type_columns,
    AlterType a_type)
    : table_name(table_name),
      database_name(database_name),
      dropped_columns(dropped_columns),
      type(a_type) {
  this->added_columns =
      std::unique_ptr<catalog::Schema>(new catalog::Schema(*added_columns));
  this->changed_type_columns = std::unique_ptr<catalog::Schema>(
      new catalog::Schema(*changed_type_columns));
}

AlterPlan::AlterPlan(const std::string &database_name,
                     const std::string &table_name, const std::string &old_name,
                     const std::string &new_name, const AlterType a_type)
    : table_name(table_name),
      database_name(database_name),
      old_name_(old_name),
      new_name_(new_name),
      type(a_type) {}

AlterPlan::AlterPlan(parser::AlterTableStatement *parse_tree) {
  table_name = parse_tree->GetTableName();
  database_name = parse_tree->GetDatabaseName();
  schema_name = parse_tree->GetSchemaName();
  switch (parse_tree->type) {
    case parser::AlterTableStatement::AlterTableType::RENAME: {
      old_name_ = parse_tree->oldName;
      new_name_ = parse_tree->newName;
      type = AlterType::RENAME;
      break;
    }
    case parser::AlterTableStatement::AlterTableType::ALTER: {
      // deal with dropped columns
      type = AlterType::ALTER;
      for (auto col : parse_tree->dropped_names) {
        LOG_TRACE("Drooped column name: %s", col.c_str());
        dropped_columns.push_back(col);
      }

      // deal with added columns
      std::vector<catalog::Column> columns;
      for (size_t i = 0; i < parse_tree->added_columns.size(); i++) {
        type::TypeId val = parser::ColumnDefinition::GetValueType(
            parse_tree->added_columns[i].get()->type);

        auto column = catalog::Column(
            val, type::Type::GetTypeSize(val),
            std::string(parse_tree->added_columns[i].get()->name));

        if (parse_tree->added_columns[i].get()->not_null) {
          catalog::Constraint constraint(ConstraintType::NOTNULL,
                                         "con_not_null");
          column.AddConstraint(constraint);
        }
        columns.emplace_back(column);
      }
      added_columns =
          std::unique_ptr<catalog::Schema>(new catalog::Schema(columns));
      columns.clear();
      // deal with change column types
      for (size_t i = 0; i < parse_tree->changed_type_columns.size(); i++) {
        auto &tmp = parse_tree->changed_type_columns[i];
        type::TypeId val =
            parser::ColumnDefinition::GetValueType(tmp.get()->type);
        auto column = catalog::Column(
            val, type::Type::GetTypeSize(val),
            std::string(parse_tree->changed_type_columns[i].get()->name));

        if (parse_tree->changed_type_columns[i].get()->not_null) {
          catalog::Constraint constraint(ConstraintType::NOTNULL,
                                         "con_not_null");
          column.AddConstraint(constraint);
        }
        columns.emplace_back(column);
      }
      changed_type_columns =
          std::unique_ptr<catalog::Schema>(new catalog::Schema(columns));
      break;
    }
    default:
      LOG_ERROR("Not Implemented the plan yet!");
      type = AlterType::INVALID;
  }
}

}  // namespace planner
}  // namespace peloton
