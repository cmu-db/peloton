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
 * @brief Transform the common things shared by all Scan types:
 * generic predicates and projections.
 *
 * @param[out]  parent  Set to a functional projection plan node if one is needed,
 *              or NULL otherwise.
 *
 * @param[out]  predicate   Set to the transformed Expression based on qual.
 *
 * @param[out]  out_col_list  Set to the output column list if pg_proj_info contains only
 *              direct mapping of attributes. Otherwise set to straightforward pass-thru list.
 *
 * @param[in]   pg_proj_info  Postgres ProjectionInfo in PlanState
 * @param[in]   qual    Postgres predicates.
 * @param[in]   out_column_count    The column count of expected output schema. It is used to
 *              skip junk attributes in Postgres.
 *
 * @return      Nothing.
 */
void PlanTransformer::TransformGenericScanInfo(
    planner::AbstractPlanNode*& parent,
    expression::AbstractExpression*& predicate,
    std::vector<oid_t>& out_col_list,
    List* qual,
    const ProjectionInfo *pg_proj_info,
    oid_t out_column_count) {

  parent = nullptr;
  predicate = nullptr;
  out_col_list.clear();

  /* Transform predicate */
  predicate = BuildPredicateFromQual(qual);

  /* Transform project info */
  std::unique_ptr<const planner::ProjectInfo> project_info(BuildProjectInfo(pg_proj_info, out_column_count));

  /*
   * Based on project_info, see whether we should create a functional projection node
   * on top, or simply pushed in an output column list.
   */
  if(nullptr == project_info.get()){  // empty predicate, pass thru
    LOG_INFO("No projections (all pass through).");

    std::vector<oid_t> column_ids;
    column_ids.resize(out_column_count);
    std::iota(column_ids.begin(), column_ids.end(), 0);
    out_col_list = std::move(column_ids);

    assert(out_col_list.size() == out_column_count);
  }
  else if(project_info->GetTargetList().size() > 0){  // Have non-trivial projection, add a plan node
    LOG_ERROR("Sorry we don't handle non-trivial projections for now. So I just output one column.\n");

    std::vector<oid_t> column_ids;
    column_ids.emplace_back(0);
    out_col_list = std::move(column_ids);
  }
  else {  // Pure direct map
    assert(project_info->GetTargetList().size() == 0);

    LOG_INFO("Pure direct map projection.\n");

    std::vector<oid_t> column_ids;
    column_ids = BuildColumnListFromDirectMap(project_info->GetDirectMapList());
    out_col_list = std::move(column_ids);

    assert(out_col_list.size() == out_column_count);
  }
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
    const ProjectionInfo *pg_pi,
    oid_t column_count) {

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

    oid_t col_id = static_cast<oid_t>(resind);

    auto peloton_expr = ExprTransformer::TransformExpr(gstate->arg);

    LOG_INFO("Target list : column id %u, Expression : \n%s\n", col_id, peloton_expr->DebugInfo().c_str());

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
 * @brief Transform a PG qual list to an expression tree.
 *
 * @return Expression tree, null if empty.
 */
expression::AbstractExpression*
PlanTransformer::BuildPredicateFromQual(List* qual){

  expression::AbstractExpression* predicate =
      ExprTransformer::TransformExpr(
          reinterpret_cast<ExprState*>(qual) );
  LOG_INFO("Predicate:\n%s \n", (nullptr==predicate)? "NULL" : predicate->DebugInfo().c_str());

  return predicate;
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
    assert(map.second.first == 0);
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
