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

#include "planner/project_info.h"
#include "common/types.h"

#include "storage/data_table.h"
#include "catalog/bootstrapper.h"
#include "parser/statement_update.h"
#include "parser/table_ref.h"
#include "expression/expression_util.h"

namespace peloton {
namespace planner {

UpdatePlan::UpdatePlan(storage::DataTable *table,
                       std::unique_ptr<const planner::ProjectInfo> project_info)
    : target_table_(table),
      project_info_(std::move(project_info)),
      updates(NULL),
      where(NULL) {
	if(updates) {
		updates->clear();
	}
	else {
		updates = new std::vector<parser::UpdateClause *>();
	}
}

UpdatePlan::UpdatePlan(parser::UpdateStatement *parse_tree) {
  auto t_ref = parse_tree->table;

  LOG_INFO("THIS IS THE SIZE OF THE UPDATE -------------> %lu",parse_tree->updates->size());
  table_name = std::string(t_ref->name);
  target_table_ = catalog::Bootstrapper::global_catalog->GetTableFromDatabase(
      DEFAULT_DB_NAME, table_name);

  
  updates = new std::vector<parser::UpdateClause*>();
  for(auto update_clause : *parse_tree->updates) {
    auto uc = new parser::UpdateClause();
    uc->column = update_clause->column;
    uc->value = update_clause->Copy()->value;

    updates->push_back(uc);
  }
  TargetList tlist;
  DirectMapList dmlist;
  oid_t col_id;
  auto schema = target_table_->GetSchema();

  std::vector<oid_t> columns;
  for (auto update : *updates) {

    // get oid_t of the column and push it to the vector;
    col_id = schema->GetColumnID(std::string(update->column));

    LOG_INFO("This is the column ID -------------------------> %d", col_id);

    columns.push_back(col_id);
    tlist.emplace_back(col_id, update->value->Copy());
  }

  for (uint i = 0; i < schema->GetColumns().size(); i++) {
    if (schema->GetColumns()[i].column_name !=
        schema->GetColumns()[col_id].column_name)
      dmlist.emplace_back(i, std::pair<oid_t, oid_t>(0, i));
  }

  std::unique_ptr<const planner::ProjectInfo> project_info(
      new planner::ProjectInfo(std::move(tlist), std::move(dmlist)));
  project_info_ = std::move(project_info);

  where = parse_tree->where->Copy();
  ReplaceColumnExpressions(target_table_->GetSchema(), where);

  std::unique_ptr<planner::SeqScanPlan> seq_scan_node(
      new planner::SeqScanPlan(target_table_, where, columns));
  AddChild(std::move(seq_scan_node));
}

void UpdatePlan::SetParameterValues(std::vector<Value> *values) {
  LOG_INFO("Setting values for parameters in updates");
  for(auto update_expr : *updates) {
    LOG_INFO("*******CHECKING UP UPDATES******");
	  expression::ExpressionUtil::ConvertParameterExpressions(update_expr->value->Copy(), values);
  }
  LOG_INFO("Setting values for parameters in where");
  expression::ExpressionUtil::ConvertParameterExpressions(where, values);
}

}  // namespace planner
}  // namespace peloton
