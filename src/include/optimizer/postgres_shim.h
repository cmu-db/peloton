//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// postgres_shim.h
//
// Identification: src/optimizer/postgres_shim.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/optimizer.h"
#include "optimizer/query_operators.h"

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Compatibility with Postgres
//===--------------------------------------------------------------------===//

bool ShouldPelotonOptimize(std::string parse);

std::shared_ptr<Select> PostgresQueryToPelotonQuery(std::string parse);

std::shared_ptr<planner::AbstractPlan>
PelotonOptimize(Optimizer &optimizer,
                std::string parse,
                int cursorOptions,
                std::vector<int> boundParams);

} /* namespace optimizer */
} /* namespace peloton */
