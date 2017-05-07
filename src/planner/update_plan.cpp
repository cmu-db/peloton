//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// drop_plan.cpp
//
// Identification: src/planner/drop_plan.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/update_plan.h"

#include "parser/update_statement.h"
#include "planner/project_info.h"
#include "type/types.h"

#include "catalog/catalog.h"
#include "expression/expression_util.h"
#include "parser/table_ref.h"
#include "storage/data_table.h"

#include "planner/abstract_plan.h"
#include "planner/abstract_scan_plan.h"
#include "planner/index_scan_plan.h"
#include "planner/seq_scan_plan.h"

#include "planner/abstract_plan.h"
#include "planner/abstract_scan_plan.h"
#include "planner/index_scan_plan.h"
#include "planner/seq_scan_plan.h"

namespace peloton {
namespace planner {

UpdatePlan::UpdatePlan(storage::DataTable *table,
                       std::unique_ptr<const planner::ProjectInfo> project_info)
    : target_table_(table),
      project_info_(std::move(project_info)),
      where_(NULL),
      update_primary_key_(false) {
  LOG_TRACE("Creating an Update Plan");

  if (project_info_ != nullptr) {
    for (const auto target : project_info_->GetTargetList()) {
      auto col_id = target.first;
      update_primary_key_ =
          target_table_->GetSchema()->GetColumn(col_id).IsPrimary();
      if (update_primary_key_)
        break;
    }
  }
}

// FIXME: Should remove when the simple_optimizer tears down
//  Initializes the update plan without adding any child nodes and
//  retrieves column ids for the child scan plan.
void UpdatePlan::BuildInitialUpdatePlan(
    const parser::UpdateStatement *parse_tree, std::vector<oid_t> &column_ids) {
  LOG_TRACE("Creating an Update Plan");
  auto t_ref = parse_tree->table;
  auto table_name = std::string(t_ref->GetTableName());
  auto database_name = t_ref->GetDatabaseName();
  LOG_TRACE("Update database %s table %s", database_name, table_name.c_str());
  target_table_ = catalog::Catalog::GetInstance()->GetTableWithName(
      database_name, table_name);
  PL_ASSERT(target_table_ != nullptr);

  for (auto update_clause : *parse_tree->updates) {
    updates_.push_back(update_clause->Copy());
  }
  TargetList tlist;
  DirectMapList dmlist;
  oid_t col_id;
  auto schema = target_table_->GetSchema();

  for (auto update : updates_) {
    // get oid_t of the column and push it to the vector;
    col_id = schema->GetColumnID(std::string(update->column));
    column_ids.push_back(col_id);
    auto *update_expr = update->value->Copy();
    expression::ExpressionUtil::TransformExpression(target_table_->GetSchema(),
                                                    update_expr);

    planner::DerivedAttribute attribute;
    attribute.expr = update_expr;
    attribute.attribute_info.type = update_expr->GetValueType();
    attribute.attribute_info.name = update->column;
    tlist.emplace_back(col_id, attribute);
  }

  auto &schema_columns = schema->GetColumns();
  for (uint i = 0; i < schema_columns.size(); i++) {
    bool is_in_target_list = false;
    for (auto col_id : column_ids) {
      if (schema_columns[i].column_name == schema_columns[col_id].column_name) {
        is_in_target_list = true;
        break;
      }
    }
    if (is_in_target_list == false)
      dmlist.emplace_back(i, std::pair<oid_t, oid_t>(0, i));
  }

  std::unique_ptr<const planner::ProjectInfo> project_info(
      new planner::ProjectInfo(std::move(tlist), std::move(dmlist)));
  project_info_ = std::move(project_info);

  if (parse_tree->where != nullptr)
    where_ = parse_tree->where->Copy();
  else
    where_ = nullptr;
  expression::ExpressionUtil::TransformExpression(target_table_->GetSchema(),
                                                  where_);
}

// FIXME: Should remove when the simple_optimizer tears down
// Creates the update plan with sequential scan.
UpdatePlan::UpdatePlan(const parser::UpdateStatement *parse_tree)
    : update_primary_key_(false) {
  std::vector<oid_t> column_ids;
  BuildInitialUpdatePlan(parse_tree, column_ids);

  // Set primary key update flag
  for (auto update_clause : updates_) {
    std::string column_name = update_clause->column;

    oid_t column_id = target_table_->GetSchema()->GetColumnID(column_name);
    update_primary_key_ =
        target_table_->GetSchema()->GetColumn(column_id).IsPrimary();
  }

  LOG_TRACE("Creating a sequential scan plan");
  std::unique_ptr<planner::SeqScanPlan> seq_scan_node(new planner::SeqScanPlan(
      target_table_, where_ != nullptr ? where_->Copy() : nullptr, column_ids));
  AddChild(std::move(seq_scan_node));
}

// FIXME: Should remove when the simple_optimizer tears down
// Creates the update plan with index scan.
UpdatePlan::UpdatePlan(const parser::UpdateStatement *parse_tree,
                       std::vector<oid_t> &key_column_ids,
                       std::vector<ExpressionType> &expr_types,
                       std::vector<type::Value> &values, oid_t &index_id)
    : update_primary_key_(false) {
  std::vector<oid_t> column_ids;
  BuildInitialUpdatePlan(parse_tree, column_ids);

  // Set primary key update flag
  for (auto update_clause : updates_) {
    std::string column_name = update_clause->column;

    oid_t column_id = target_table_->GetSchema()->GetColumnID(column_name);
    update_primary_key_ =
        target_table_->GetSchema()->GetColumn(column_id).IsPrimary();
  }

  // Create index scan desc
  std::vector<expression::AbstractExpression *> runtime_keys;
  auto index = target_table_->GetIndex(index_id);
  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      index, key_column_ids, expr_types, values, runtime_keys);
  // Create plan node.
  LOG_TRACE("Creating a index scan plan");
  auto predicate_cpy = where_ == nullptr ? nullptr : where_->Copy();
  std::unique_ptr<planner::IndexScanPlan> index_scan_node(
      new planner::IndexScanPlan(target_table_, predicate_cpy, column_ids,
                                 index_scan_desc, true));
  LOG_TRACE("Index scan plan created");
  AddChild(std::move(index_scan_node));
}

void UpdatePlan::SetParameterValues(std::vector<type::Value> *values) {
  LOG_TRACE("Setting parameter values in Update");

  auto &children = GetChildren();
  // One sequential scan
  children[0]->SetParameterValues(values);
}

}  // namespace planner
}  // namespace peloton
