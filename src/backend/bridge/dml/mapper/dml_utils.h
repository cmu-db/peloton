//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// ddl_utils.h
//
// Identification: src/backend/bridge/ddl/ddl_utils.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/planner/abstract_plan.h"

#include "postgres.h"
#include "c.h"
#include "nodes/execnodes.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// DML UTILS
//===--------------------------------------------------------------------===//

class DMLUtils {
 public:
  DMLUtils(const DMLUtils &) = delete;
  DMLUtils &operator=(const DMLUtils &) = delete;
  DMLUtils(DMLUtils &&) = delete;
  DMLUtils &operator=(DMLUtils &&) = delete;

  static peloton::planner::AbstractPlanState *peloton_prepare_data(PlanState *planstate);

};

}  // namespace bridge
}  // namespace peloton
