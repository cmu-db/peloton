
#include "backend/bridge/dml/mapper/mapper.h"
#include "backend/planner/aggregate_node.h"

#include "access/htup_details.h"
#include "catalog/pg_aggregate.h"
#include "utils/syscache.h"


namespace peloton {
namespace bridge {


planner::AbstractPlanNode*
PlanTransformer::TransformAgg(const AggState *plan_state){
  const AggState* agg_state = plan_state;

  LOG_INFO("Number of Agg nodes: %d \n", agg_state->numaggs);
  LOG_INFO("Number of Agg phases: %d \n", agg_state->numphases);

  int aggno = 0;

//  ListCell   *l;
//  foreach(l, agg_state->aggs)
//  {
//    AggrefExprState *aggrefstate = (AggrefExprState *) lfirst(l);
//    Aggref *aggref = (Aggref *) aggrefstate->xprstate.expr;
//
//    auto aggTuple = SearchSysCache1(AGGFNOID,
//                                    ObjectIdGetDatum(aggref->aggfnoid));
//
//    auto aggform = (Form_pg_aggregate) GETSTRUCT(aggTuple);
//
//    auto pg_func_oid = aggform->aggtransfn;
//
//    ReleaseSysCache(aggTuple);
//
//    LOG_INFO("Aggno : %d , PG_Func_Oid = %u \n", ++aggno, pg_func_oid);
//
//  }

  return nullptr;
}


}
}
