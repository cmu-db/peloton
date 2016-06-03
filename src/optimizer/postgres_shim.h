//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// postgres_shim.h
//
// Identification: src/backend/optimizer/postgres_shim.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/optimizer/optimizer.h"
#include "backend/optimizer/query_operators.h"

#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "nodes/params.h"

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Compatibility with Postgres
//===--------------------------------------------------------------------===//

bool ShouldPelotonOptimize(Query *parse);

std::shared_ptr<Select> PostgresQueryToPelotonQuery(Query *parse);

std::shared_ptr<planner::AbstractPlan>
PelotonOptimize(Optimizer &optimizer,
                Query *parse,
                int cursorOptions,
                ParamListInfo boundParams);

} /* namespace optimizer */
} /* namespace peloton */
