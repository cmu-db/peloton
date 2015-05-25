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
#include <cassert>

TupleTableSlot *NStoreExecute(PlanState *planstate){

  assert(planstate->plan);
  printf("nstore node tag : %d \n", nodeTag(planstate));

  return nullptr;
}
