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
                      std::unique_ptr<const planner::ProjectInfo> project_info)
      : target_table_(table), project_info_(std::move(project_info)) {}

UpdatePlan::UpdatePlan(parser::UpdateStatement *parse_tree) {
	auto t_ref = parse_tree->table;
	table_name = std::string(t_ref->name);
	updates = parse_tree->updates;
	where = parse_tree->where;

}

}  // namespace planner
}  // namespace peloton
