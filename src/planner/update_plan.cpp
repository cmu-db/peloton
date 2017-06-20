//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// update_plan.cpp
//
// Identification: src/planner/update_plan.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/update_plan.h"
#include "planner/project_info.h"
#include "storage/data_table.h"
#include "type/types.h"

namespace peloton {
namespace planner {

UpdatePlan::UpdatePlan(storage::DataTable *table,
                       std::unique_ptr<const planner::ProjectInfo> project_info)
    : target_table_(table),
      project_info_(std::move(project_info)),
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

void UpdatePlan::SetParameterValues(std::vector<type::Value> *values) {
  LOG_TRACE("Setting parameter values in Update");
  auto &children = GetChildren();
  // One sequential scan
  children[0]->SetParameterValues(values);
}

void UpdatePlan::PerformBinding(BindingContext &binding_context) {
  BindingContext input_context;

  const auto &children = GetChildren();
  PL_ASSERT(children.size() == 1);

  children[0]->PerformBinding(input_context);

  // Do projection (if one exists)
  if (GetProjectInfo() != nullptr) {
    std::vector<const BindingContext *> inputs = {&input_context};
    GetProjectInfo()->PerformRebinding(binding_context, inputs);
  }
}

}  // namespace planner
}  // namespace peloton
