/*-------------------------------------------------------------------------
 *
 * nested_loop_join.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/executor/nested_loop_join_executor.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/catalog/schema.h"
#include "backend/executor/abstract_executor.h"
#include "backend/planner/nested_loop_join_node.h"

#include <vector>

namespace peloton {
namespace executor {

class NestedIndexLoopJoinExecutor : public AbstractExecutor {
    NestedIndexLoopJoinExecutor(const NestedIndexLoopJoinExecutor &) = delete;
    NestedIndexLoopJoinExecutor& operator=(const NestedIndexLoopJoinExecutor &) = delete;

public:
    explicit NestedIndexLoopJoinExecutor(planner::AbstractPlanNode *node);

protected:
    bool DInit();

    bool DExecute();

private:

    //===--------------------------------------------------------------------===//
    // Executor State
    //===--------------------------------------------------------------------===//

    /** @brief Result of nested loop join. */
    std::vector<LogicalTile *> result;

    /** @brief Starting left table scan. */
    bool left_scan_start = false;

    //===--------------------------------------------------------------------===//
    // Plan Info
    //===--------------------------------------------------------------------===//

    /** @brief Join predicate. */
    const expression::AbstractExpression *predicate_ = nullptr;

};

} // namespace executor
} // namespace peloton
