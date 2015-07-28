/*-------------------------------------------------------------------------
 *
 * mapper_utils.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/bridge/dml/mapper/mapper_utils.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/bridge/dml/mapper/mapper.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Utils
//===--------------------------------------------------------------------===//

const ValueArray PlanTransformer::BuildParams(const ParamListInfo param_list) {
  ValueArray params;
  if (param_list != nullptr) {
    params.Reset(param_list->numParams);
    ParamExternData *postgres_param = param_list->params;
    for (int i = 0; i < params.GetSize(); ++i, ++postgres_param) {
      params[i] = TupleTransformer::GetValue(postgres_param->value,
                                             postgres_param->ptype);
    }
  }

  LOG_TRACE("Built %d params: \n%s", params.GetSize(), params.Debug());
  return params;
}

/**
 * @brief Transform a PG ProjectionInfo structure to a Peloton ProjectInfo
 *object.
 *
 * @param pg_proj_info  The PG ProjectionInfo struct to be transformed
 * @param column_count  The valid column count of output. This is used to
 * skip junk attributes in PG.
 * @return  An ProjectInfo object built from the PG ProjectionInfo.
 */
const planner::ProjectInfo *PlanTransformer::BuildProjectInfo(
    const ProjectionInfo *pg_pi, oid_t column_count) {
  if (pg_pi == nullptr) return nullptr;

  /*
   * (A) Transform non-trivial target list
   */
  planner::ProjectInfo::TargetList target_list;

  ListCell *tl;

  foreach (tl, pg_pi->pi_targetlist) {
    GenericExprState *gstate = (GenericExprState *)lfirst(tl);
    TargetEntry *tle = (TargetEntry *)gstate->xprstate.expr;
    AttrNumber resind = tle->resno - 1;

    if (!(resind < column_count)) continue;  // skip junk attributes

    LOG_TRACE("Target list : column id : %u , Top-level (pg) expr tag : %u \n",
              resind, nodeTag(gstate->arg->expr));

    oid_t col_id = static_cast<oid_t>(resind);

    // TODO: Somebody should be responsible for freeing the expression tree.
    auto peloton_expr = ExprTransformer::TransformExpr(gstate->arg);

    target_list.emplace_back(col_id, peloton_expr);
  }

  /*
   * (B) Transform direct map list
   * Special case:
   * a null constant may be specified in SimpleVars by PG,
   * in case of that, we add a Target to target_list we created above.
   */
  planner::ProjectInfo::DirectMapList direct_map_list;

  if (pg_pi->pi_numSimpleVars > 0) {
    int numSimpleVars = pg_pi->pi_numSimpleVars;
    bool *isnull = pg_pi->pi_slot->tts_isnull;
    int *varSlotOffsets = pg_pi->pi_varSlotOffsets;
    int *varNumbers = pg_pi->pi_varNumbers;

    if (pg_pi->pi_directMap)  // Sequential direct map
    {
      /* especially simple case where vars go to output in order */
      for (int i = 0; i < numSimpleVars && i < column_count; i++) {
        oid_t tuple_idx =
            (varSlotOffsets[i] == offsetof(ExprContext, ecxt_innertuple) ? 1
                                                                         : 0);
        int varNumber = varNumbers[i] - 1;
        oid_t in_col_id = static_cast<oid_t>(varNumber);
        oid_t out_col_id = static_cast<oid_t>(i);

        if (!(isnull[i])) {  // Non null, direct map
          direct_map_list.emplace_back(
              out_col_id, planner::ProjectInfo::DirectMap::second_type(
                              tuple_idx, in_col_id));
        } else {  // NUll, constant, added to target_list
          Value null = ValueFactory::GetNullValue();
          target_list.emplace_back(out_col_id,
                                   expression::ConstantValueFactory(null));
        }

        LOG_INFO("Input column : %u , Output column : %u \n", in_col_id,
                 out_col_id);
      }
    } else  // Non-sequential direct map
    {
      /* we have to pay attention to varOutputCols[] */
      int *varOutputCols = pg_pi->pi_varOutputCols;

      for (int i = 0; i < numSimpleVars; i++) {
        oid_t tuple_idx =
            (varSlotOffsets[i] == offsetof(ExprContext, ecxt_innertuple) ? 1
                                                                         : 0);
        int varNumber = varNumbers[i] - 1;
        int varOutputCol = varOutputCols[i] - 1;
        oid_t in_col_id = static_cast<oid_t>(varNumber);
        oid_t out_col_id = static_cast<oid_t>(varOutputCol);

        if (!(isnull[out_col_id])) {  // Non null, direct map
          direct_map_list.emplace_back(
              out_col_id, planner::ProjectInfo::DirectMap::second_type(
                              tuple_idx, in_col_id));
        } else {  // NUll, constant
          Value null = ValueFactory::GetNullValue();
          target_list.emplace_back(out_col_id,
                                   expression::ConstantValueFactory(null));
        }

        LOG_INFO("Input column : %u , Output column : %u \n", in_col_id,
                 out_col_id);
      }
    }
  }

  return new planner::ProjectInfo(std::move(target_list),
                                  std::move(direct_map_list));
}

/**
 * @brief Transform a DirectMapList to a one-dimensional column list.
 * This is intended to incorporate a pure-direct-map projection into a scan.
 * The caller should make sure the direct map list has output columns positions.
 * from 0 ~ N-1
 */
const std::vector<oid_t>
PlanTransformer::BuildColumnListFromDirectMap(planner::ProjectInfo::DirectMapList dmlist){
  std::sort(dmlist.begin(), dmlist.end(),
            [](const planner::ProjectInfo::DirectMap &a,
               const planner::ProjectInfo::DirectMap &b){
                return a.first < b.first; }
            );

  assert(dmlist.front().first == 0);
  assert(dmlist.back().first == dmlist.size()-1 );

  std::vector<oid_t> rv;

  for(auto map : dmlist) {
    rv.emplace_back(map.second.second);
  }

  return rv;
}


/**
 * Convet a Postgres JoinType into a Peloton JoinType
 *
 * We may want to have a uniform JoinType enum, instead of a transformation
 */
PelotonJoinType PlanTransformer::TransformJoinType(const JoinType type) {
  switch (type) {
    case JOIN_INNER:
      return JOIN_TYPE_INNER;
    case JOIN_FULL:
      return JOIN_TYPE_OUTER;
    case JOIN_LEFT:
      return JOIN_TYPE_LEFT;
    case JOIN_RIGHT:
      return JOIN_TYPE_RIGHT;
    default:
      return JOIN_TYPE_INVALID;
  }
}

}  // namespace bridge
}  // namespace peloton
