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

namespace peloton {
namespace planner {

UpdatePlan::UpdatePlan(storage::DataTable *table,
		std::unique_ptr<const planner::ProjectInfo> project_info) :
		target_table_(table), project_info_(std::move(project_info)), updates(
				NULL), where(NULL) {
}

UpdatePlan::UpdatePlan(parser::UpdateStatement *parse_tree) {
	auto t_ref = parse_tree->table;
	table_name = std::string(t_ref->name);
	target_table_ = catalog::Bootstrapper::global_catalog->GetTableFromDatabase(
			DEFAULT_DB_NAME, table_name);
	updates = parse_tree->updates;

	TargetList tlist;
	DirectMapList dmlist;

	auto column_id = target_table_->GetSchema()->GetColumns();

	std::vector<oid_t> columns;
	for (auto update : *updates) {

		auto column = std::string(update->column);
		oid_t col_id = 0;

		for (uint i = 0; i < column_id.size(); i++) {
			if (column == column_id[i].GetName())
				col_id = i;
			break;
		}

		// get oid_t of the column and push it to the vector;
		tlist.emplace_back(col_id, update->value);
	}

	for (uint i = 0; i < tlist.size(); i++) {
		dmlist.emplace_back(tlist[i].first,
				std::pair<oid_t, oid_t>(0, tlist[i].first));
	}

	std::unique_ptr<const planner::ProjectInfo> project_info(
			new planner::ProjectInfo(std::move(tlist), std::move(dmlist)));
	project_info_ = std::move(project_info);
	
	where = parse_tree->where;

	auto right_of = (expression::AbstractExpression*)where->GetRight();
	auto left_of = (expression::AbstractExpression*)where->GetLeft();

	auto predicate = new expression::ComparisonExpression<expression::CmpGt>(
      EXPRESSION_TYPE_COMPARE_EQUAL, right_of, left_of);
	std::unique_ptr<planner::SeqScanPlan> seq_scan_node(
			new planner::SeqScanPlan(target_table_, predicate, columns));
	this->AddChild(std::move(seq_scan_node));
}

}  // namespace planner
}  // namespace peloton
