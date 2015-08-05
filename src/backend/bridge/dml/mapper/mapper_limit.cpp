//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// mapper_limit.cpp
//
// Identification: src/backend/bridge/dml/mapper/mapper_limit.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "backend/bridge/dml/mapper/mapper.h"
#include "backend/planner/limit_node.h"

extern Datum ExecEvalExprSwitchContext(ExprState *expression,
                                       ExprContext *econtext, bool *isNull,
                                       ExprDoneCond *isDone);

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Limit
//===--------------------------------------------------------------------===//

/**
 * @brief Convert a Postgres LimitState into a Peloton LimitPlanNode
 *        does not support LIMIT ALL
 *        does not support cases where there is only OFFSET
 * @return Pointer to the constructed AbstractPlanNode
 */
planner::AbstractPlanNode *
PlanTransformer::TransformLimit(const LimitState *node) {
  ExprContext *econtext = node->ps.ps_ExprContext;
  Datum val;
  bool isNull;
  int64 limit;
  int64 offset;
  bool noLimit;
  bool noOffset;

  /* Resolve limit and offset */
  if (node->limitOffset) {
    val = ExecEvalExprSwitchContext(node->limitOffset, econtext, &isNull, NULL);
    /* Interpret NULL offset as no offset */
    if (isNull)
      offset = 0;
    else {
      offset = DatumGetInt64(val);
      if (offset < 0) {
        LOG_ERROR("OFFSET must not be negative, offset = %ld", offset);
      }
      noOffset = false;
    }
  } else {
    /* No OFFSET supplied */
    offset = 0;
  }

  if (node->limitCount) {
    val = ExecEvalExprSwitchContext(node->limitCount, econtext, &isNull, NULL);
    /* Interpret NULL count as no limit (LIMIT ALL) */
    if (isNull) {
      limit = 0;
      noLimit = true;
    } else {
      limit = DatumGetInt64(val);
      if (limit < 0) {
        LOG_ERROR("LIMIT must not be negative, limit = %ld", limit);
      }
      noLimit = false;
    }
  } else {
    /* No LIMIT supplied */
    limit = 0;
    noLimit = true;
  }

  /* TODO: does not do pass down bound to child node
   * In Peloton, they are both unsigned. But both of them cannot be negative,
   * The is safe */
  /* TODO: handle no limit and no offset cases, in which the corresponding value
   * is 0 */
  LOG_INFO("Flags :: Limit: %d, Offset: %d", noLimit, noOffset);
  LOG_INFO("Limit: %ld, Offset: %ld", limit, offset);
  auto plan_node = new planner::LimitNode(limit, offset);

  /* Resolve child plan */
  PlanState *subplan_state = outerPlanState(node);
  assert(subplan_state != nullptr);
  plan_node->AddChild(PlanTransformer::TransformPlan(subplan_state));
  return plan_node;
}

} // namespace bridge
} // namespace peloton
