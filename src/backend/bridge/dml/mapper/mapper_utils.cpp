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
#include "backend/planner/projection_plan.h"

namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Utils
//===--------------------------------------------------------------------===//

const ValueArray PlanTransformer::BuildParams(const ParamListInfo param_list) {
  ValueArray params;
  if (param_list != nullptr) {
    params.reset(param_list->numParams);
    ParamExternData *postgres_param = param_list->params;
    for (int i = 0; i < params.GetSize(); ++i, ++postgres_param) {
      params[i] = TupleTransformer::GetValue(postgres_param->value,
                                             postgres_param->ptype);
    }

    assert(params.GetSize() > 0);
  }

  LOG_INFO("Built %d params: \n%s", params.GetSize(), params.Debug().c_str());
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
    planner::AbstractPlan *&parent, expression::AbstractExpression *&predicate,
    std::vector<oid_t> &out_col_list, const AbstractScanPlanState *sstate,
    bool use_projInfo) {

  List *qual = sstate->qual;

  parent = nullptr;
  predicate = nullptr;
  out_col_list.clear();

  /* Transform predicate */
  predicate = BuildPredicateFromQual(qual);

  LOG_TRACE("out_column_count = %d", out_column_count);

  /* Transform project info */
  std::unique_ptr<const planner::ProjectInfo> project_info(nullptr);
  if (use_projInfo) {
    project_info.reset(BuildProjectInfoFromTLSkipJunk(sstate->targetlist));
  }

  LOG_INFO("project_info : %s",
           project_info.get() ? project_info->Debug().c_str() : "<NULL>\n");

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
        sstate->tts_tupleDescriptor);

    parent = new planner::ProjectionPlan(project_info.release(),
                                         project_schema);

  }

  else {  // Pure direct map
    assert(project_info->GetTargetList().size() == 0);
    assert(project_info->GetDirectMapList().size() > 0);

    LOG_TRACE("Pure direct map projection.\n");

    std::vector<oid_t> column_ids;
    column_ids = BuildColumnListFromDirectMap(project_info->GetDirectMapList());
    out_col_list = std::move(column_ids);

    //assert(out_col_list.size() == out_column_count);
    // TODO: sometimes, these two do not equal due to junk attributes.
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
    const PelotonProjectionInfo *pg_pi) {

  if (pg_pi == nullptr) {
    LOG_TRACE("pg proj info is null, no projection");
    return nullptr;
  }

  // (A) Construct target list
  planner::ProjectInfo::TargetList target_list;
  ListCell *item;
  std::vector<oid_t> expr_col_ids;

  foreach (item, pg_pi->expr_col_ids)
  {
    oid_t expr_col_id = lfirst_int(item);
    expr_col_ids.push_back(expr_col_id);
  }

  oid_t list_itr = 0;
  foreach (item, pg_pi->expr_states)
  {
    ExprState *expr_state = (ExprState *) lfirst(item);

    auto peloton_expr = ExprTransformer::TransformExpr(expr_state);
    auto expr_col_id = expr_col_ids[list_itr];

    if (peloton_expr == nullptr) {
      LOG_TRACE("Seems to be a row value expression. Skipped.");
      continue;

    }

    LOG_TRACE("Target : column id %u, Expression : \n%s", expr_col_id,
              peloton_expr->DebugInfo().c_str());

    target_list.emplace_back(expr_col_id, peloton_expr);
    list_itr++;
  }

  // (B) Construct direct map list
  planner::ProjectInfo::DirectMapList direct_map_list;
  std::vector<oid_t> out_col_ids, tuple_idxs, in_col_ids;

  size_t col_count;
  foreach (item, pg_pi->out_col_ids)
  {
    oid_t out_col_id = lfirst_int(item);
    out_col_ids.push_back(out_col_id);
  }
  col_count = out_col_ids.size();
  LOG_TRACE("Direct Map :: COL COUNT :: %lu \n", out_col_ids.size());

  foreach (item, pg_pi->tuple_idxs)
  {
    oid_t tuple_idx = lfirst_int(item);
    tuple_idxs.push_back(tuple_idx);
  }
  assert(col_count == tuple_idxs.size());
  foreach (item, pg_pi->in_col_ids)
  {
    oid_t in_col_id = lfirst_int(item);
    in_col_ids.push_back(in_col_id);
  }
  assert(col_count == in_col_ids.size());

  for (oid_t col_itr = 0; col_itr < col_count; col_itr++) {
    auto out_col_id = out_col_ids[col_itr];
    auto tuple_idx = tuple_idxs[col_itr];
    auto in_col_id = in_col_ids[col_itr];

    direct_map_list.emplace_back(out_col_id,
                                 std::make_pair(tuple_idx, in_col_id));
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
      LOG_TRACE(
          "Invalid / Junk attribute. Skipped.  resno : %u , resjunk : %u \n",
          tle->resno, tle->resjunk);
      continue;  // skip junk attributes
    }

    oid_t col_id = static_cast<oid_t>(resind);

    auto peloton_expr = ExprTransformer::TransformExpr(gstate->arg);

    if (peloton_expr == nullptr) {
      LOG_TRACE("Seems to be a row value expression. Skipped.");
      continue;
    }

    LOG_TRACE("Target : column id %u, Expression : \n%s", col_id,

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
  LOG_INFO("Predicate:\n%s \n",
            (nullptr == predicate) ? "NULL" : predicate->DebugInfo(" ").c_str());

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
 * @brief Build a Peloton ProjectInfo from PG PlanState.targetlist.
 * 1. The target list will be separated into generic targets and direct maps.
 * 2. Junk attributes will be \b removed.
 *
 * NB: The 1) above is overlapped with PG function ExecBuildProjectionInfo(),
 * however, that function doesn't do 2). What's worse, it loses the information
 * about junk attributes for direct maps.
 */
const planner::ProjectInfo*
PlanTransformer::BuildProjectInfoFromTLSkipJunk(List *targetList) {
  planner::ProjectInfo::TargetList target_list;
  planner::ProjectInfo::DirectMapList direct_map_list;
  ListCell* tl;

  foreach(tl, targetList)
  {
    GenericExprState *gstate = (GenericExprState *) lfirst(tl);
    TargetEntry *tle = (TargetEntry *) gstate->xprstate.expr;

    if (tle->resjunk
        || !AttributeNumberIsValid(
            tle->resno) || !AttrNumberIsForUserDefinedAttr(tle->resno)) {
      LOG_TRACE("Skip junk / invalid attribute. \n");

      continue;  // SKIP junk / invalid attributes.
    }

    Var *variable = (Var *) gstate->arg->expr;
    bool isSimpleVar = false;

    if (variable != NULL && IsA(variable, Var) && variable->varattno > 0) {
      isSimpleVar = true;
    }

    if (isSimpleVar) {
      AttrNumber attnum = variable->varattno;

      oid_t input_col_id = static_cast<oid_t>(attnum - 1);
      oid_t output_col_id = static_cast<oid_t>(tle->resno - 1);
      oid_t tuple_idx = 0;

      switch (variable->varno) {
        case INNER_VAR:
          tuple_idx = 1;
          break;

        case OUTER_VAR:
          break;

        default:
          break;
      }

      // Add it to direct map list
      direct_map_list.emplace_back(output_col_id,
                                   std::make_pair(tuple_idx, input_col_id));
    } else {
      /* Not a simple variable, add it to generic targetlist */
      oid_t output_col_id = static_cast<oid_t>(tle->resno - 1);
      auto peloton_expr = ExprTransformer::TransformExpr(gstate->arg);

      assert(peloton_expr);

      target_list.emplace_back(output_col_id, peloton_expr);
    }
  }  // end foreach

  if (target_list.empty() && direct_map_list.empty()) {
    return nullptr;
  }

  return new planner::ProjectInfo(std::move(target_list),
                                  std::move(direct_map_list));

}

/**
 * Convert a Postgres JoinType into a Peloton JoinType
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
