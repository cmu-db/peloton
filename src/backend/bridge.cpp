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

#include "backend/outfuncs.h"
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

  nstore::LOG_INFO("\nPLAN :: %s \n",
                   nstore::backend::NodeToString(node->plan).c_str());

  return result;
}

namespace nstore {
namespace backend {

void Bridge::ProcessPlan(Plan *plan){

  if(plan == nullptr)
    return;

  ProcessPlan(plan->lefttree);
  ProcessPlan(plan->righttree);
}


} // namespace backend
} // namespace nstore
