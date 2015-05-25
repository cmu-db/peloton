/*-------------------------------------------------------------------------
 *
 * bridge.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/backend/bridge.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "postgres.h"
#include "executor/execdebug.h"

//===--------------------------------------------------------------------===//
// Bridge between PostgreSQL and N-Store
//===--------------------------------------------------------------------===//

extern "C" TupleTableSlot *NStoreExecute(PlanState *planstate);

namespace nstore {
namespace backend {

// Main handler for query plan
class Bridge {

 public:

  static void ProcessPlan(Plan *plan);

};


} // namespace backend
} // namespace nstore
