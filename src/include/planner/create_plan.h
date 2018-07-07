//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// create_plan.h
//
// Identification: src/include/planner/create_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/create_statement.h"
#include "planner/abstract_plan.h"

namespace peloton {
namespace catalog {
class Schema;
}
namespace storage {
class DataTable;
}
namespace parser {
class CreateStatement;
}

namespace Expression {
class AbstractExpression;
}

namespace planner {

/**
 * The meta-data for a foreign key reference.
 * This is meant to be a bridge from the parser to the
 * catalog. It only has table names and not OIDs, whereas
 * the catalog only wants OIDs.
 */
struct ForeignKeyInfo {
  std::vector<std::string> foreign_key_sources;
  std::vector<std::string> foreign_key_sinks;
  std::string sink_table_name;
  std::string constraint_name;
  FKConstrActionType upd_action;
  FKConstrActionType del_action;
};

class CreatePlan : public AbstractPlan {
 public:
  CreatePlan() = delete;

  // This construnctor is for Create Database Test used only
  explicit CreatePlan(std::string database_name, CreateType c_type);

  // This construnctor is for copy() used only
  explicit CreatePlan(std::string table_name, std::string schema_name,
                      std::string database_name,
                      std::unique_ptr<catalog::Schema> schema,
                      CreateType c_type);

  explicit CreatePlan(parser::CreateStatement *parse_tree);

  inline PlanNodeType GetPlanNodeType() const { return PlanNodeType::CREATE; }

  const std::string GetInfo() const { return "Create Plan"; }

  std::unique_ptr<AbstractPlan> Copy() const {
    return std::unique_ptr<AbstractPlan>(new CreatePlan(
        table_name, schema_name, database_name,
        std::unique_ptr<catalog::Schema>(table_schema), create_type));
  }

  std::string GetIndexName() const { return index_name; }

  std::string GetTableName() const { return table_name; }

  std::string GetSchemaName() const { return schema_name; }

  std::string GetDatabaseName() const { return database_name; }

  catalog::Schema *GetSchema() const { return table_schema; }

  CreateType GetCreateType() const { return create_type; }

  bool IsUnique() const { return unique; }

  IndexType GetIndexType() const { return index_type; }

  std::vector<std::string> GetIndexAttributes() const { return index_attrs; }

  inline std::vector<ForeignKeyInfo> GetForeignKeys() const {
    return foreign_keys;
  }
  std::vector<oid_t> GetKeyAttrs() const { return key_attrs; }

  void SetKeyAttrs(std::vector<oid_t> p_key_attrs) { key_attrs = p_key_attrs; }

  // interfaces for triggers

  std::string GetTriggerName() const { return trigger_name; }

  std::vector<std::string> GetTriggerFuncName() const {
    return trigger_funcname;
  }

  std::vector<std::string> GetTriggerArgs() const { return trigger_args; }

  std::vector<std::string> GetTriggerColumns() const { return trigger_columns; }

  // Remeber to release the pointer
  expression::AbstractExpression *GetTriggerWhen() const;

  int16_t GetTriggerType() const { return trigger_type; }

 protected:
  // This is a helper method for extracting foreign key information
  // and storing it in an internal struct.
  void ProcessForeignKeyConstraint(const std::string &table_name,
                                   const parser::ColumnDefinition *col);

 private:
  // Table Name
  std::string table_name;

  // namespace Name
  std::string schema_name;

  // Database Name
  std::string database_name;

  // Table Schema
  catalog::Schema *table_schema;

  // Index attributes
  std::vector<std::string> index_attrs;
  std::vector<oid_t> key_attrs;

  // Check to either Create Table or INDEX
  CreateType create_type;

  // IndexName
  std::string index_name;

  // Index Tyoe specified from parser (Default: SKIPLIST)
  IndexType index_type;

  // UNIQUE INDEX flag
  bool unique;

  // ColumnDefinition for multi-column constraints (including foreign key)
  std::vector<ForeignKeyInfo> foreign_keys;
  std::string trigger_name;
  std::vector<std::string> trigger_funcname;
  std::vector<std::string> trigger_args;
  std::vector<std::string> trigger_columns;
  std::unique_ptr<expression::AbstractExpression> trigger_when = nullptr;
  int16_t trigger_type;  // information about row, timing, events, access by
                         // pg_trigger

 private:
  DISALLOW_COPY_AND_MOVE(CreatePlan);
};

}  // namespace planner
}  // namespace peloton
