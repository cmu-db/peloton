/*-------------------------------------------------------------------------
 *
 * mapper_target_list.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/bridge/dml/mapper/mapper_target_list.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/bridge/dml/mapper/mapper.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Target List
//===--------------------------------------------------------------------===//

/**
 * @brief Transforms a non-trivial projection target list
 * (ProjectionInfo.pi_targetList) in Postgres
 * to a Peloton one.
 */
std::vector<std::pair<oid_t, expression::AbstractExpression*>>
TransformTargetList(List* target_list, oid_t column_count) {

  std::vector<std::pair<oid_t, expression::AbstractExpression*>> proj_list;
  ListCell *tl;

  foreach(tl, target_list)
  {
    GenericExprState *gstate = (GenericExprState *) lfirst(tl);
    TargetEntry *tle = (TargetEntry *) gstate->xprstate.expr;
    AttrNumber resind = tle->resno - 1;

    if (!(resind < column_count))
      continue;  // skip junk attributes

    LOG_INFO("Target list : column id : %u , Top-level (pg) expr tag : %u \n",
             resind, nodeTag(gstate->arg->expr));

    oid_t col_id = static_cast<oid_t>(resind);

    // TODO: Somebody should be responsible for freeing the expression tree.
    auto peloton_expr = ExprTransformer::TransformExpr(gstate->arg);

    proj_list.emplace_back(col_id, peloton_expr);
  }

  return proj_list;
}

} // namespace bridge
} // namespace peloton

