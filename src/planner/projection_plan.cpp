
//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// projection_plan.cpp
//
// Identification: /peloton/src/planner/projection_plan.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
 
#include "planner/projection_plan.h"
#include "common/types.h"
#include "planner/project_info.h"
#include "catalog/schema.h"

namespace peloton{
namespace planner{

ProjectionPlan::ProjectionPlan(std::unique_ptr<const planner::ProjectInfo> &&project_info,
                 std::shared_ptr<const catalog::Schema> &schema)
      : project_info_(std::move(project_info)), schema_(schema) {}
}
}


