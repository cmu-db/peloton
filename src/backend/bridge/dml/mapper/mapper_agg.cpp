#include "backend/bridge/dml/expr/pg_func_map.h"
#include "backend/bridge/dml/mapper/mapper.h"
#include "backend/bridge/ddl/schema_transformer.h"
#include "backend/planner/aggregateV2_node.h"

#include "access/htup_details.h"
#include "catalog/pg_aggregate.h"
#include "executor/nodeAgg.h"
#include "utils/syscache.h"

namespace peloton {
namespace bridge {

planner::AbstractPlanNode*
PlanTransformer::TransformAgg(const AggState *plan_state) {
  const AggState* agg_state = plan_state;
  auto agg = reinterpret_cast<const Agg*>(agg_state->ss.ps.plan);

  auto num_phases = agg_state->numphases;
  LOG_INFO("Number of Agg phases: %d \n", num_phases);

  assert(1 == num_phases);  // When we'll have >1 phases?

  /* Get project info */
  std::unique_ptr<const planner::ProjectInfo> proj_info(
      BuildProjectInfoFromTLSkipJunk(agg_state->ss.ps.targetlist));
  LOG_INFO("proj_info : \n%s", proj_info->Debug().c_str());

  /* Get predicate */
  std::unique_ptr<const expression::AbstractExpression> predicate(
      BuildPredicateFromQual(agg_state->ss.ps.qual));

  /* Get Aggregate terms */
  std::vector<planner::AggregateV2Node::AggTerm> unique_agg_terms;

  auto num_aggs = agg_state->numaggs;  // number of unique aggregates
  LOG_INFO("Number of (unique) Agg nodes: %d \n", num_aggs);
  for (int aggno = 0; aggno < num_aggs; aggno++) {
    auto transfn_oid = agg_state->peragg[aggno].transfn_oid;

    auto itr = peloton::bridge::kPgTransitFuncMap.find(transfn_oid);
    if (kPgFuncMap.end() == itr) {
      LOG_ERROR("Unmapped Transit function Id : %u\n", transfn_oid);
      return nullptr;
    }

    // We don't check whether the mapped exprtype is a valid aggregate type here.
    PltFuncMetaInfo fn_meta = itr->second;
    // We only take the first argument as input to aggregator
    GenericExprState *gstate = (GenericExprState *) lfirst(
        list_head(agg_state->peragg[aggno].aggrefstate->args));
    auto agg_expr = ExprTransformer::TransformExpr(gstate->arg);

    unique_agg_terms.emplace_back(fn_meta.exprtype, agg_expr);

    LOG_INFO(
        "Unique Agg # : %d , transfn_oid : %u\n , aggtype = %s \n expr = %s",
        aggno, transfn_oid, ExpressionTypeToString(fn_meta.exprtype).c_str(),
        agg_expr->DebugInfo().c_str());
  }

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
      SchemaTransformer::GetSchemaFromTupleDesc(
          agg_state->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor));

  auto retval = new planner::AggregateV2Node(proj_info.release(),
                                             predicate.release(),
                                             std::move(unique_agg_terms),
                                             std::move(groupby_col_ids),
                                             output_schema.release(),
                                             AGGREGATE_TYPE_HASH);

  // Find children
  auto lchild = TransformPlan(outerPlanState(agg_state));
  retval->AddChild(lchild);

  return retval;
}

}
}
