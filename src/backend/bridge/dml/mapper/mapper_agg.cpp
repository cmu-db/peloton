
#include "backend/bridge/dml/mapper/mapper.h"
#include "backend/bridge/ddl/schema_transformer.h"
#include "backend/planner/aggregate_node.h"

#include "access/htup_details.h"
#include "catalog/pg_aggregate.h"
#include "executor/nodeAgg.h"
#include "utils/syscache.h"


namespace peloton {
namespace bridge {


planner::AbstractPlanNode*
PlanTransformer::TransformAgg(const AggState *plan_state){
  const AggState* agg_state = plan_state;

  auto num_aggs = agg_state->numaggs; // number of unique aggregates
  auto num_phases = agg_state->numphases;

  LOG_INFO("Number of (unique) Agg nodes: %d \n", num_aggs);
  LOG_INFO("Number of Agg phases: %d \n", num_phases);

  for(int aggno = 0; aggno < num_aggs; aggno++){
    auto transfn_oid = agg_state->peragg[aggno].transfn_oid;
    LOG_INFO("Unique Agg # : %d , transfn_oid : %u \n", aggno, transfn_oid);
  }

  auto output_schema = SchemaTransformer::GetSchemaFromTupleDesc(
          agg_state->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor);

  auto target_list = BuildTargetList(agg_state->ss.ps.targetlist, output_schema->GetColumnCount());


  return nullptr;
}


}
}
