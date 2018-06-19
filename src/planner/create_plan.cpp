//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// create_plan.cpp
//
// Identification: src/planner/create_plan.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/create_plan.h"

#include "common/internal_types.h"
#include "expression/abstract_expression.h"
#include "expression/constant_value_expression.h"
#include "storage/data_table.h"

namespace peloton {
namespace planner {

CreatePlan::CreatePlan(std::string database_name, CreateType c_type)
    : database_name(database_name), create_type(c_type) {}

CreatePlan::CreatePlan(std::string table_name, std::string schema_name,
                       std::string database_name,
                       std::unique_ptr<catalog::Schema> schema,
                       CreateType c_type)
    : table_name(table_name),
      schema_name(schema_name),
      database_name(database_name),
      table_schema(schema.release()),
      create_type(c_type) {}

CreatePlan::CreatePlan(parser::CreateStatement *parse_tree) {
  switch (parse_tree->type) {
    case parser::CreateStatement::CreateType::kDatabase: {
      create_type = CreateType::DB;
      database_name = std::string(parse_tree->GetDatabaseName());
      break;
    }

    case parser::CreateStatement::CreateType::kSchema: {
      create_type = CreateType::SCHEMA;
      database_name = std::string(parse_tree->GetDatabaseName());
      schema_name = std::string(parse_tree->GetSchemaName());
      break;
    }

    case parser::CreateStatement::CreateType::kTable: {
      table_name = std::string(parse_tree->GetTableName());
      schema_name = std::string(parse_tree->GetSchemaName());
      database_name = std::string(parse_tree->GetDatabaseName());
      std::vector<catalog::Column> columns;
      std::vector<std::string> pri_cols;

      create_type = CreateType::TABLE;

      for (auto &col : parse_tree->columns) {
        type::TypeId val = col->GetValueType(col->type);

        LOG_TRACE("Column name: %s.%s; Is primary key: %d", table_name.c_str(),
                  col->name.c_str(), col->primary);

        // Create column
        auto column = catalog::Column(val, type::Type::GetTypeSize(val),
                                      std::string(col->name), false);
        if (!column.IsInlined()) {
          column.SetLength(col->varlen);
        }

        // Add NOT NULL constraints to the column
        if (col->not_null) {
          column.SetNotNull();
          LOG_TRACE("Added a not-null constraint on column \"%s.%s\"",
                    table_name.c_str(), col->name.c_str());
        }

        // Add DEFAULT constraints to the column
        if (col->default_value != nullptr) {
          // Referenced from insert_plan.cpp
          if (col->default_value->GetExpressionType() !=
              ExpressionType::VALUE_PARAMETER) {
            expression::ConstantValueExpression *const_expr_elem =
                dynamic_cast<expression::ConstantValueExpression *>(
                    col->default_value.get());
            column.SetDefaultValue(const_expr_elem->GetValue());
            LOG_TRACE("Added a default constraint %s on column \"%s.%s\"",
                      const_expr_elem->GetValue().ToString().c_str(),
                      table_name.c_str(), col->name.c_str());
          }
        }

        columns.push_back(column);

        // Collect Multi-column constraints information
        // TODO: Following constraints info in ColumnDefinition should be
        // independent
        //       for multi-column constraints like foreign key.

        // Primary key
        if (col->primary) {
          pri_cols.push_back(col->name);
        }

        // Unique constraint
        // Currently only supports for single column
        if (col->unique) {
          ProcessUniqueConstraint(col.get());
        }

        // Check expression constraint
        // Currently only supports simple boolean forms like (a > 0)
        if (col->check_expression != nullptr) {
          ProcessCheckConstraint(col.get());
        }
      }

      catalog::Schema *schema = new catalog::Schema(columns);

      // The parser puts the multi-column constraint information
      // into an artificial ColumnDefinition.
      // primary key constraint
      if (pri_cols.size() > 0) {
        primary_key.primary_key_cols = pri_cols;
        primary_key.constraint_name = "con_primary";
        has_primary_key = true;
        LOG_TRACE("Added a primary key constraint on column \"%s\"",
                  table_name.c_str());
      }

      // foreign key
      for (auto &fk : parse_tree->foreign_keys) {
        ProcessForeignKeyConstraint(table_name, fk.get());
      }

      // TODO: UNIQUE and CHECK constraints

      table_schema = schema;
      break;
    }
    case parser::CreateStatement::CreateType::kIndex: {
      create_type = CreateType::INDEX;
      index_name = std::string(parse_tree->index_name);
      table_name = std::string(parse_tree->GetTableName());
      schema_name = std::string(parse_tree->GetSchemaName());
      database_name = std::string(parse_tree->GetDatabaseName());

      // This holds the attribute names.
      // This is a fix for a bug where
      // The vector<char*>* items gets deleted when passed
      // To the Executor.

      std::vector<std::string> index_attrs_holder;

      for (auto &attr : parse_tree->index_attrs) {
        index_attrs_holder.push_back(attr);
      }

      index_attrs = index_attrs_holder;

      index_type = parse_tree->index_type;

      unique = parse_tree->unique;
      break;
    }

    case parser::CreateStatement::CreateType::kTrigger: {
      create_type = CreateType::TRIGGER;
      trigger_name = std::string(parse_tree->trigger_name);
      table_name = std::string(parse_tree->GetTableName());
      schema_name = std::string(parse_tree->GetSchemaName());
      database_name = std::string(parse_tree->GetDatabaseName());

      if (parse_tree->trigger_when) {
        trigger_when.reset(parse_tree->trigger_when->Copy());
      } else {
        trigger_when.reset();
      }
      trigger_type = parse_tree->trigger_type;

      for (auto &s : parse_tree->trigger_funcname) {
        trigger_funcname.push_back(s);
      }
      for (auto &s : parse_tree->trigger_args) {
        trigger_args.push_back(s);
      }
      for (auto &s : parse_tree->trigger_columns) {
        trigger_columns.push_back(s);
      }

      break;
    }
    default:
      LOG_ERROR("UNKNOWN CREATE TYPE");
      // TODO Should we handle this here?
      break;
  }

  // TODO check type CreateType::kDatabase
}

void CreatePlan::ProcessForeignKeyConstraint(
    const std::string &table_name, const parser::ColumnDefinition *col) {
  ForeignKeyInfo fkey_info;

  // Extract source and sink column names
  for (auto &key : col->foreign_key_source) {
    fkey_info.foreign_key_sources.push_back(key);
  }
  for (auto &key : col->foreign_key_sink) {
    fkey_info.foreign_key_sinks.push_back(key);
  }

  // Extract table names
  fkey_info.sink_table_name = col->fk_sink_table_name;

  // Extract delete and update actions
  fkey_info.upd_action = col->foreign_key_update_action;
  fkey_info.del_action = col->foreign_key_delete_action;

  fkey_info.constraint_name =
      "FK_" + table_name + "->" + fkey_info.sink_table_name;

  LOG_DEBUG("Added a foreign key constraint toward sink table %s",
            fkey_info.sink_table_name.c_str());
  foreign_keys.push_back(fkey_info);
}

void CreatePlan::ProcessUniqueConstraint(const parser::ColumnDefinition *col) {
  UniqueInfo unique_info;

  unique_info.unique_cols = {col->name};
  unique_info.constraint_name = "con_unique";

  LOG_TRACE("Added a unique constraint on column \"%s.%s\"", table_name.c_str(),
            col->name.c_str());
  con_uniques.push_back(unique_info);
}

void CreatePlan::ProcessCheckConstraint(const parser::ColumnDefinition *col) {
  CheckInfo check_info;

  // TODO: more expression types need to be supported
  if (col->check_expression->GetValueType() == type::TypeId::BOOLEAN) {
    check_info.check_cols.push_back(col->name);

    const expression::ConstantValueExpression *const_expr_elem =
        dynamic_cast<const expression::ConstantValueExpression *>(
            col->check_expression->GetChild(1));
    type::Value tmp_value = const_expr_elem->GetValue();

    check_info.exp =
        std::make_pair(std::move(col->check_expression->GetExpressionType()),
                       std::move(tmp_value));

    check_info.constraint_name = "con_check";

    LOG_TRACE("Added a check constraint on column \"%s\"", table_name.c_str());
    con_checks.push_back(check_info);
  }
}

expression::AbstractExpression *CreatePlan::GetTriggerWhen() const {
  if (trigger_when) {
    return trigger_when->Copy();
  } else {
    return nullptr;
  }
}

}  // namespace planner
}  // namespace peloton
