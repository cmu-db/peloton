//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// mapper_utils.cpp
//
// Identification: src/backend/bridge/dml/mapper/mapper_utils.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/bridge/dml/mapper/mapper.h"
#include "backend/bridge/ddl/schema_transformer.h"
#include "backend/planner/projection_node.h"

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

  LOG_TRACE("Built %d params: \n%s", params.GetSize(), params.Debug().c_str());
  return params;
}

/**
 * @brief Extract the common things shared by all Scan types:
 * generic predicates and projections.
 *
 * @param[out]  parent  Set to a created projection plan node if one is needed,
 *              or NULL otherwise.
 *
 * @param[out]  predicate   Set to the transformed Expression based on qual. Or
 *NULL if so is qual.
 *
 * @param[out]  out_col_list  Set to the output column list if ps_ProjInfo
 *contains only
 *              direct mapping of attributes. \b Empty if no direct map is
 *presented.
 *
 * @param[in]   sstate  The ScanState from which generic things are extracted.
 *
 * @param[in]   use_projInfo  Parse projInfo or not. Sometimes the projInfo of a
 *Scan may have been
 *              stolen by its parent.
 *
 * @return      Nothing.
 */
void PlanTransformer::GetGenericInfoFromScanState(
    planner::AbstractPlanNode *&parent,
    expression::AbstractExpression *&predicate,
    std::vector<oid_t> &out_col_list, const ScanState *sstate,
    bool use_projInfo) {
  List *qual = sstate->ps.qual;
  const ProjectionInfo *pg_proj_info = sstate->ps.ps_ProjInfo;
  oid_t out_column_count = static_cast<oid_t>(sstate->ps.ps_ResultTupleSlot
      ->tts_tupleDescriptor->natts);

  parent = nullptr;
  predicate = nullptr;
  out_col_list.clear();

  /* Transform predicate */
  predicate = BuildPredicateFromQual(qual);

  /* Transform project info */
  std::unique_ptr<const planner::ProjectInfo> project_info(nullptr);
  if (use_projInfo) {
    project_info.reset(BuildProjectInfo(pg_proj_info, out_column_count));
  }

  /*
   * Based on project_info, see whether we should create a functional projection
   * node
   * on top, or simply pushed in an output column list.
   */
  if (nullptr == project_info.get()) {  // empty predicate, or ignore projInfo, pass thru
    LOG_TRACE("No projections (all pass through)");

    assert(out_col_list.size() == 0);
  } else if (project_info->GetTargetList().size() > 0) {  // Have non-trivial projection, add a plan node
    LOG_TRACE(
        "Non-trivial projections are found. Projection node will be " "created. \n");

    auto project_schema = SchemaTransformer::GetSchemaFromTupleDesc(
        sstate->ps.ps_ResultTupleSlot->tts_tupleDescriptor);

    parent = new planner::ProjectionNode(project_info.release(),
                                         project_schema);

  }

  else {  // Pure direct map
    assert(project_info->GetTargetList().size() == 0);
    assert(project_info->GetDirectMapList().size() > 0);

    LOG_TRACE("Pure direct map projection.\n");

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
 *          NULL if no valid project info is found.
 *
 * @warning Some projections in PG may be ignored.
 * For example, the "row" projection
 */
const planner::ProjectInfo *PlanTransformer::BuildProjectInfo(
    const ProjectionInfo *pg_pi, oid_t column_count) {
  if (pg_pi == nullptr) {
    LOG_TRACE("pg proj info is null, no projection");
    return nullptr;
  }

  /*
   * (A) Transform non-trivial target list
   */
  planner::ProjectInfo::TargetList target_list = BuildTargetList(
      pg_pi->pi_targetlist, column_count);

  /*
   * (B) Transform direct map list
   * Special case:
   * a null constant may be specified in SimpleVars by PG,
   * in case of that, we add a Target to target_list we created above.
   */
  planner::ProjectInfo::DirectMapList direct_map_list;

  if (pg_pi->pi_numSimpleVars > 0) {
    int numSimpleVars = pg_pi->pi_numSimpleVars;
    int *varSlotOffsets = pg_pi->pi_varSlotOffsets;
    int *varNumbers = pg_pi->pi_varNumbers;

    if (pg_pi->pi_directMap)  // Sequential direct map
    {
      /* especially simple case where vars go to output in order */
      for (int i = 0; i < numSimpleVars && i < column_count; i++) {
        oid_t tuple_idx = (
            varSlotOffsets[i] == offsetof(ExprContext, ecxt_innertuple) ? 1 : 0);
        int varNumber = varNumbers[i] - 1;
        oid_t in_col_id = static_cast<oid_t>(varNumber);
        oid_t out_col_id = static_cast<oid_t>(i);

        direct_map_list.emplace_back(out_col_id,
                                     std::make_pair(tuple_idx, in_col_id));

        LOG_TRACE("Input column : %u , Output column : %u", in_col_id,
                  out_col_id);
      }
    } else  // Non-sequential direct map
    {
      /* we have to pay attention to varOutputCols[] */
      int *varOutputCols = pg_pi->pi_varOutputCols;

      for (int i = 0; i < numSimpleVars; i++) {
        oid_t tuple_idx = (
            varSlotOffsets[i] == offsetof(ExprContext, ecxt_innertuple) ? 1 : 0);
        int varNumber = varNumbers[i] - 1;
        int varOutputCol = varOutputCols[i] - 1;
        oid_t in_col_id = static_cast<oid_t>(varNumber);
        oid_t out_col_id = static_cast<oid_t>(varOutputCol);

        direct_map_list.emplace_back(out_col_id,
                                     std::make_pair(tuple_idx, in_col_id));

        LOG_TRACE("Input column : %u , Output column : %u \n", in_col_id,
                  out_col_id);
      }
    }
  }

  if (target_list.empty() && direct_map_list.empty())
    return nullptr;

  return new planner::ProjectInfo(std::move(target_list),
                                  std::move(direct_map_list));
}

/**
 * Transform a target list.
 */
const planner::ProjectInfo::TargetList PlanTransformer::BuildTargetList(
    const List* targetList, oid_t column_count) {

  planner::ProjectInfo::TargetList target_list;

  ListCell *tl;

  foreach (tl, targetList)
  {
    GenericExprState *gstate = (GenericExprState *) lfirst(tl);
    TargetEntry *tle = (TargetEntry *) gstate->xprstate.expr;
    AttrNumber resind = tle->resno - 1;

    if (!(resind < column_count && AttributeNumberIsValid(tle->resno)
        && AttrNumberIsForUserDefinedAttr(tle->resno) && !tle->resjunk)) {
      LOG_INFO("Invalid / Junk attribute. Skipped.");
      continue;  // skip junk attributes
    }

    oid_t col_id = static_cast<oid_t>(resind);

    auto peloton_expr = ExprTransformer::TransformExpr(gstate->arg);

    if (peloton_expr == nullptr) {
      LOG_INFO("Seems to be a row value expression. Skipped.");
      continue;
    }

    LOG_INFO("Target : column id %u, Expression : \n%s", col_id,
             peloton_expr->DebugInfo().c_str());

    target_list.emplace_back(col_id, peloton_expr);
  }

  return std::move(target_list);

}

/**
 * @brief Transform a PG qual list to an expression tree.
 *
 * @return Expression tree, null if empty.
 */
expression::AbstractExpression *PlanTransformer::BuildPredicateFromQual(
    List *qual) {
  expression::AbstractExpression *predicate = ExprTransformer::TransformExpr(
      reinterpret_cast<ExprState *>(qual));
  LOG_TRACE("Predicate:\n%s \n",
            (nullptr == predicate) ? "NULL" : predicate->DebugInfo().c_str());

  return predicate;
}

/**
 * @brief Transform a DirectMapList to a one-dimensional column list.
 * This is intended to incorporate a pure-direct-map projection into a scan.
 * The caller should make sure the direct map list has output columns positions.
 * from 0 ~ N-1
 */
const std::vector<oid_t> PlanTransformer::BuildColumnListFromDirectMap(
    planner::ProjectInfo::DirectMapList dmlist) {
  std::sort(dmlist.begin(), dmlist.end(),
            [](const planner::ProjectInfo::DirectMap &a,
                const planner::ProjectInfo::DirectMap &b) {
              return a.first < b.first;
            });

  assert(dmlist.front().first == 0);
  assert(dmlist.back().first == dmlist.size() - 1);

  std::vector<oid_t> rv;

  for (auto map : dmlist) {
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
