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

#include <include/expression/constant_value_expression.h>
#include "planner/create_plan.h"

#include "parser/create_statement.h"
#include "storage/data_table.h"
#include "catalog/schema.h"
#include "catalog/column.h"

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
      // TODO: Currently, the parser will parse the foreign key constraint and
      // put it into a ColumnDefinition. Later when we implement constraint
      // we may need to change this. Just skip foreign key constraint for now
      if (col->type == parser::ColumnDefinition::FOREIGN)
        continue;
        
      type::Type::TypeId val = col->GetValueType(col->type);

      LOG_TRACE("Column name: %s; Is primary key: %d", col->name, col->primary);

      // Check main constraints
      if (col->primary) {
        catalog::Constraint constraint(ConstraintType::PRIMARY, "con_primary");
        column_constraints.push_back(constraint);
        LOG_DEBUG("Added a primary key constraint on column \"%s\"", col->name);
      }

      if (col->not_null) {
        catalog::Constraint constraint(ConstraintType::NOTNULL, "con_not_null");
        column_constraints.push_back(constraint);
        LOG_DEBUG("Added a not-null constraint on column \"%s\"", col->name);
      }

      if (col->unique) {
        catalog::Constraint constraint(ConstraintType::UNIQUE, "con_unique");
        column_constraints.push_back(constraint);
        LOG_DEBUG("Added a unique constraint on column \"%s\"", col->name);
      }

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
          LOG_DEBUG("Added a default constraint %s on column \"%s\"", v.ToString().c_str(), col->name);
        }
      }

      // Check expression constraint
      // Currently only supports simple boolean forms like (a > 0)
      if (col->check_expression != nullptr) {
        // TODO: more expression types need to be supported
        if (col->check_expression->GetValueType() == type::Type::TypeId::BOOLEAN) {
          catalog::Constraint constraint(ConstraintType::CHECK, "con_check");

          const expression::ConstantValueExpression *const_expr_elem =
            dynamic_cast<const expression::ConstantValueExpression *>(col->check_expression->GetChild(1));

          type::Value tmp_value = const_expr_elem->GetValue();
          constraint.AddCheck(std::move(col->check_expression->GetExpressionType()), std::move(tmp_value));
          column_constraints.push_back(constraint);
          LOG_DEBUG("Added a check constraint on column \"%s\"", col->name);
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

}  // namespace planner
}  // namespace peloton
