/*-------------------------------------------------------------------------
 *
 * bridge.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/backend/bridge.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/bridge.h"
#include "common/logger.h"

#include <cassert>

/* ----------------------------------------------------------------
 *    Based on ExecProcNode in execProcnode.c
 *
 *    Execute the given node to return a(nother) tuple.
 * ----------------------------------------------------------------
 */
TupleTableSlot *NStoreExecute(PlanState *node){
  TupleTableSlot *result = NULL;

  fprintf(stderr, "NStoreExecute: \n");

  //nstore::backend::Bridge::ProcessPlan(node->plan);

  return result;
}

namespace nstore {
namespace backend {

void Bridge::ProcessPlan(Plan *plan){

  if(plan == nullptr)
    return;

  fprintf(stderr, "Plan node type: %d \n", (int) nodeTag(plan->type));

  ProcessPlan(plan->lefttree);
  ProcessPlan(plan->righttree);

}


} // namespace backend
} // namespace nstore
