//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// create_plan.cpp
//
// Identification: src/planner/create_plan.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/create_plan.h"

#include "expression/constant_value_expression.h"
#include "storage/data_table.h"
#include "type/types.h"

namespace peloton {
namespace planner {

CreatePlan::CreatePlan(storage::DataTable *table) {
  target_table_ = table;
  table_schema = nullptr;
}

CreatePlan::CreatePlan(std::string name, std::string database_name,
                       std::unique_ptr<catalog::Schema> schema,
                       CreateType c_type) {
  table_name = name;
  this->database_name = database_name;
  table_schema = schema.release();
  create_type = c_type;
}

CreatePlan::CreatePlan(parser::CreateStatement *parse_tree) {
  table_name = parse_tree->GetTableName();
  database_name = parse_tree->GetDatabaseName();
  std::vector<catalog::Column> columns;
  std::vector<catalog::Constraint> column_constraints;
  if (parse_tree->type == parse_tree->CreateType::kTable) {
    create_type = CreateType::TABLE;
    for (auto col : *parse_tree->columns) {
      // The parser puts the Foreign Key information into an artificial
      // ColumnDefinition.
      if (col->type == parser::ColumnDefinition::DataType::FOREIGN) {
        this->ProcessForeignKeyConstraint(table_name, col);
        // XXX: Why should we always continue here?
        continue;
      }
  
      type::TypeId val = col->GetValueType(col->type);

      LOG_TRACE("Column name: %s.%s; Is primary key: %d", table_name.c_str(), col->name, col->primary);

      // Check main constraints
      if (col->primary) {
        catalog::Constraint constraint(ConstraintType::PRIMARY, "con_primary");
        column_constraints.push_back(constraint);
        LOG_TRACE("Added a primary key constraint on column \"%s.%s\"", table_name.c_str(), col->name);
      }

      if (col->not_null) {
        catalog::Constraint constraint(ConstraintType::NOTNULL, "con_not_null");
        column_constraints.push_back(constraint);
        LOG_TRACE("Added a not-null constraint on column \"%s.%s\"", table_name.c_str(), col->name);
      }

      if (col->unique) {
        catalog::Constraint constraint(ConstraintType::UNIQUE, "con_unique");
        column_constraints.push_back(constraint);
        LOG_TRACE("Added a unique constraint on column \"%s.%s\"", table_name.c_str(), col->name);
      }
      
      // TODO: check if foreign key just on column
      if (col->foreign_key_source != nullptr) {
        LOG_TRACE("FK source: %lu", col->foreign_key_source->size());
      }
      if (col->foreign_key_sink != nullptr) {
        LOG_TRACE("FK sink: %lu", col->foreign_key_sink->size());
      }
      /* **************** */

      // Add the default value
      if (col->default_value != nullptr) {
        // Referenced from insert_plan.cpp
        if (col->default_value->GetExpressionType() != ExpressionType::VALUE_PARAMETER) {
          expression::ConstantValueExpression *const_expr_elem =
            dynamic_cast<expression::ConstantValueExpression *>(col->default_value);

          catalog::Constraint constraint(ConstraintType::DEFAULT, "con_default");
          type::Value v = const_expr_elem->GetValue();
          constraint.addDefaultValue(v);
          column_constraints.push_back(constraint);
          LOG_TRACE("Added a default constraint %s on column \"%s.%s\"",
                    v.ToString().c_str(), table_name.c_str(), col->name);
        }
      }

      // Check expression constraint
      // Currently only supports simple boolean forms like (a > 0)
      if (col->check_expression != nullptr) {
        // TODO: more expression types need to be supported
        if (col->check_expression->GetValueType() == type::TypeId::BOOLEAN) {
          catalog::Constraint constraint(ConstraintType::CHECK, "con_check");

          const expression::ConstantValueExpression *const_expr_elem =
            dynamic_cast<const expression::ConstantValueExpression *>(col->check_expression->GetChild(1));

          type::Value tmp_value = const_expr_elem->GetValue();
          constraint.AddCheck(std::move(col->check_expression->GetExpressionType()), std::move(tmp_value));
          column_constraints.push_back(constraint);
          LOG_TRACE("Added a check constraint on column \"%s.%s\"",
                    table_name.c_str(), col->name);
        }
      }

      auto column = catalog::Column(val, type::Type::GetTypeSize(val),
                                    std::string(col->name), false);
      for (auto con : column_constraints) {
        column.AddConstraint(con);
      }

      column_constraints.clear();
      columns.push_back(column);
    }
    catalog::Schema *schema = new catalog::Schema(columns);
    table_schema = schema;
  }
  if (parse_tree->type == parse_tree->CreateType::kIndex) {
    create_type = CreateType::INDEX;
    index_name = std::string(parse_tree->index_name);
    table_name = std::string(parse_tree->GetTableName());

    // This holds the attribute names.
    // This is a fix for a bug where
    // The vector<char*>* items gets deleted when passed
    // To the Executor.

    std::vector<std::string> index_attrs_holder;

    for (auto attr : *parse_tree->index_attrs) {
      index_attrs_holder.push_back(attr);
    }

    index_attrs = index_attrs_holder;

    index_type = parse_tree->index_type;

    unique = parse_tree->unique;
  }
  // TODO check type CreateType::kDatabase
}

void CreatePlan::ProcessForeignKeyConstraint(const std::string table_name,
                                             const parser::ColumnDefinition *col) {

  ForeignKeyInfo fkey_info;

  // Extract source and sink column names
  for (auto key : *(col->foreign_key_source)) {
    fkey_info.foreign_key_sources.push_back(key);
  }
  for (auto key : *(col->foreign_key_sink)) {
    fkey_info.foreign_key_sinks.push_back(key);
  }

  // Extract table names
  fkey_info.sink_table_name = strdup(col->table_info_->table_name);

  // Extract delete and update actions
  fkey_info.upd_action = col->foreign_key_update_action;
  fkey_info.del_action = col->foreign_key_delete_action;

  fkey_info.constraint_name =
      "FK_" + table_name + "->" + fkey_info.sink_table_name;

  LOG_DEBUG("Added a foreign key constraint toward sink table %s",
            fkey_info.sink_table_name.c_str());
  foreign_keys.push_back(fkey_info);
}

}  // namespace planner
}  // namespace peloton
