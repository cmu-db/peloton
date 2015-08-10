//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// ddl_utils.cpp
//
// Identification: src/backend/bridge/ddl/ddl_utils.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <iostream>

#include "backend/bridge/dml/mapper/dml_utils.h"
#include "backend/common/logger.h"
#include "backend/planner/abstract_plan.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// DML Utils
//===--------------------------------------------------------------------===//

/**
 * @brief preparing data
 * @param planstate
 */
peloton::planner::AbstractPlanState *DMLUtils::peloton_prepare_data(PlanState *planstate) {

  return nullptr;
}

}  // namespace bridge
}  // namespace peloton
