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

#include "common/types.h"
#include "planner/project_info.h"

#include "catalog/catalog.h"
#include "expression/expression_util.h"
#include "parser/statement_update.h"
#include "parser/table_ref.h"
#include "storage/data_table.h"

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
      where_(NULL) {
  LOG_TRACE("Creating an Update Plan");
}

//  Initializes the update plan without adding any child nodes and
//  retrieves column ids for the child scan plan.
void UpdatePlan::BuildInitialUpdatePlan(parser::UpdateStatement *parse_tree,
                                        std::vector<oid_t> &column_ids) {
  LOG_TRACE("Creating an Update Plan");
  auto t_ref = parse_tree->table;
  table_name = std::string(t_ref->GetTableName());
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
    auto update_expr = update->value->Copy();
    expression::ExpressionUtil::ReplaceColumnExpressions(
        target_table_->GetSchema(), update_expr);
    tlist.emplace_back(col_id, update_expr);
  }

  auto &schema_columns = schema->GetColumns();
  for (uint i = 0; i < schema_columns.size(); i++) {
    bool is_in_target_list = false;
    for (auto col_id : column_ids) {
      if (schema_columns[i].column_name == schema_columns[col_id].column_name)
        is_in_target_list = true;
      break;
    }
    if (is_in_target_list == false)
      dmlist.emplace_back(i, std::pair<oid_t, oid_t>(0, i));
  }

  std::unique_ptr<const planner::ProjectInfo> project_info(
      new planner::ProjectInfo(std::move(tlist), std::move(dmlist)));
  project_info_ = std::move(project_info);

  where_ = parse_tree->where->Copy();
  expression::ExpressionUtil::ReplaceColumnExpressions(
      target_table_->GetSchema(), where_);
}

// Creates the update plan with sequential scan.
UpdatePlan::UpdatePlan(parser::UpdateStatement *parse_tree) {
  std::vector<oid_t> column_ids;
  BuildInitialUpdatePlan(parse_tree, column_ids);
  LOG_TRACE("Creating a sequential scan plan");
  std::unique_ptr<planner::SeqScanPlan> seq_scan_node(
      new planner::SeqScanPlan(target_table_, where_->Copy(), column_ids));
  AddChild(std::move(seq_scan_node));
}

// Creates the update plan with index scan.
UpdatePlan::UpdatePlan(parser::UpdateStatement *parse_tree,
                       std::vector<oid_t> &key_column_ids,
                       std::vector<ExpressionType> &expr_types,
                       std::vector<common::Value> &values, oid_t &index_id) {
  std::vector<oid_t> column_ids;
  BuildInitialUpdatePlan(parse_tree, column_ids);
  // Create index scan desc
  std::vector<expression::AbstractExpression *> runtime_keys;
  auto index = target_table_->GetIndex(index_id);
  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      index, key_column_ids, expr_types, values, runtime_keys);
  // Create plan node.
  LOG_TRACE("Creating a index scan plan");
  std::unique_ptr<planner::IndexScanPlan> index_scan_node(
      new planner::IndexScanPlan(target_table_, where_, column_ids,
                                 index_scan_desc, true));
  LOG_TRACE("Index scan plan created");
  AddChild(std::move(index_scan_node));
}

void UpdatePlan::SetParameterValues(std::vector<common::Value> *values) {
  LOG_TRACE("Setting parameter values in Update");

  auto &children = GetChildren();
  // One sequential scan
  children[0]->SetParameterValues(values);
}

}  // namespace planner
}  // namespace peloton
