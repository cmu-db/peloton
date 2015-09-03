#include "backend/planner/aggregate_plan.h"
#include "backend/bridge/dml/expr/pg_func_map.h"
#include "backend/bridge/dml/mapper/mapper.h"
#include "backend/bridge/ddl/schema_transformer.h"
#include "executor/nodeAgg.h"

namespace peloton {
namespace bridge {

planner::AbstractPlan*
PlanTransformer::TransformAgg(const AggPlanState *plan_state) {

  // Alias all I need
  const Agg* agg = plan_state->agg_plan;
  auto numphases = plan_state->numphases;
  auto numaggs = plan_state->numaggs;
  auto targetlist = plan_state->ps_targetlist;
  auto qual = plan_state->ps_qual;
  auto peragg = plan_state->peragg;
  auto tupleDesc = plan_state->result_tupleDescriptor;
  auto aggstrategy = plan_state->agg_plan->aggstrategy;

  LOG_INFO("Number of Agg phases: %d \n", numphases);

  assert(1 == numphases);  // When we'll have >1 phases?

  /* Get project info */
  std::unique_ptr<const planner::ProjectInfo> proj_info(
      BuildProjectInfoFromTLSkipJunk(targetlist));
  LOG_INFO("proj_info : \n%s", proj_info->Debug().c_str());

  /* Get predicate */
  std::unique_ptr<const expression::AbstractExpression> predicate(
      BuildPredicateFromQual(qual));

  /* Get Aggregate terms */
  std::vector<planner::AggregatePlan::AggTerm> unique_agg_terms;

  LOG_INFO("Number of (unique) Agg nodes: %d \n", numaggs);
  for (int aggno = 0; aggno < numaggs; aggno++) {
    auto transfn_oid = peragg[aggno].transfn_oid;

    auto itr = peloton::bridge::kPgTransitFuncMap.find(transfn_oid);
    if (kPgFuncMap.end() == itr) {
      LOG_ERROR("Unmapped Transit function Id : %u\n", transfn_oid);
      return nullptr;
    }

    // We don't check whether the mapped exprtype is a valid aggregate type here.
    PltFuncMetaInfo fn_meta = itr->second;
    // We only take the first argument as input to aggregator because
    // we don't have multi-argument aggregator in Peloton at the moment.
    // WARNING: there can be no arguments (e.g., COUNT(*))
    auto arguments = peragg[aggno].aggrefstate->args;
    expression::AbstractExpression* agg_expr = nullptr;
    if (arguments) {
      GenericExprState *gstate = (GenericExprState *) lfirst(
          list_head(arguments));
      agg_expr = ExprTransformer::TransformExpr(gstate->arg);
    }

    /*
     * AggStatePerAggData.sortColIdx along with other related attributes
     * are used to handle ORDER BY and DISTINCT *within* aggregation.
     * E.g.,
     * SELECT count(DISTINCT x) ...
     * SELECT str_agg(y ORDER BY x) ...
     * Currently, we only handle the agg(DISTINCT x) case by
     * checking whether numDistinctCols > 0.
     * Note that numDistinctCols > 0 may be a necessary but not sufficient
     * condition for agg(DISTINCT x).
     */

    bool distinct = (peragg[aggno].numDistinctCols > 0);

    unique_agg_terms.emplace_back(fn_meta.exprtype, agg_expr, distinct);

    LOG_INFO(
        "Unique Agg # : %d , transfn_oid : %u\n , aggtype = %s \n expr = %s, numDistinctCols = %d",
        aggno, transfn_oid, ExpressionTypeToString(fn_meta.exprtype).c_str(),
        agg_expr ? agg_expr->Debug().c_str() : "<NULL>",
        peragg[aggno].numDistinctCols);

    for (int i = 0; i < peragg[aggno].numDistinctCols; i++) {
      LOG_INFO("sortColIdx[%d] : %d \n", i, peragg[aggno].sortColIdx[i]);
    }

  }  // end loop aggno

  /* Get Group by columns */
  std::vector<oid_t> groupby_col_ids;
  LOG_INFO("agg.numCols = %d", agg->numCols);
  for (int i = 0; i < agg->numCols; i++) {
    LOG_INFO("agg.grpColIdx[%d] = %d \n", i, agg->grpColIdx[i]);

    auto attrno = agg->grpColIdx[i];
    if (AttributeNumberIsValid(attrno) && AttrNumberIsForUserDefinedAttr(attrno)) {
      groupby_col_ids.emplace_back(AttrNumberGetAttrOffset(attrno));
    }
  }

  /* Get output schema */
  std::unique_ptr<catalog::Schema> output_schema(
      SchemaTransformer::GetSchemaFromTupleDesc(tupleDesc));

  /* Map agg stragegy */
  LOG_INFO(
      "aggstrategy : %s\n",
      (AGG_HASHED == aggstrategy) ? "HASH" : (AGG_SORTED ? "SORT" : "PLAIN"));

  PelotonAggType agg_type = AGGREGATE_TYPE_INVALID;

  switch (aggstrategy) {
    case AGG_SORTED:
      agg_type = AGGREGATE_TYPE_SORTED;
      break;
    case AGG_HASHED:
      agg_type = AGGREGATE_TYPE_HASH;
      break;
    case AGG_PLAIN:
      agg_type = AGGREGATE_TYPE_PLAIN;
      break;
  }

  auto column_ids = BuildColumnListFromTargetList(proj_info->GetTargetList());

  for(auto agg_term : unique_agg_terms){
    LOG_INFO("AGG TERM :: %s", agg_term.expression->Debug().c_str());
  }

  auto retval = new planner::AggregatePlan(proj_info.release(),
                                           predicate.release(),
                                           std::move(unique_agg_terms),
                                           std::move(groupby_col_ids),
                                           output_schema.release(), agg_type);

  ((planner::AggregatePlan *)retval)->SetColumnIds(column_ids);

  // Find children
  auto lchild = TransformPlan(outerAbstractPlanState(plan_state));
  retval->AddChild(lchild);

  return retval;
}

}
}
