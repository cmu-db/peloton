/*-------------------------------------------------------------------------
 *
 * peloton.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/bridge/peloton.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PELOTON_H
#define PELOTON_H

#include "backend/common/types.h"
#include "backend/common/message_queue.h"
#include "backend/bridge/ddl/bootstrap.h"
#include "backend/planner/abstract_plan.h"
#include "backend/bridge/dml/mapper/dml_utils.h"

#include "libpq/libpq-be.h"
#include "nodes/execnodes.h"
#include "utils/memutils.h"
#include "tcop/dest.h"

/* ----------
 * Peloton_Status     Sent by the peloton to share the status with backend.
 * ----------
 */
typedef struct peloton_status {
  peloton::Result m_result;
  List *m_result_slots;

  // number of tuples processed
  uint32_t m_processed;

  peloton_status(){
    m_processed = 0;
    m_result = peloton::RESULT_SUCCESS;
    m_result_slots = nullptr;
  }

} peloton_status;

extern bool logging_on;

/* ----------
 * Functions called from postmaster
 * ----------
 */

extern bool IsPelotonQuery(List *relationOids);


/* ----------
 * Functions called from postgres, utility, and execMain
 * ----------
 */

extern void peloton_bootstrap();

extern void peloton_ddl(Node *parsetree);

extern void peloton_dml(PlanState *planstate,
                        bool sendTuples,
                        DestReceiver *dest,
                        TupleDesc tuple_desc);


/* ----------
 * Stats Sent by the peloton to share the status with backend.
 * ----------
 */

struct dirty_index_info {
  Oid index_oid;
  float number_of_tuples;
};

struct dirty_table_info {
  Oid table_oid;
  float number_of_tuples;
  dirty_index_info** dirty_indexes;
  Oid dirty_index_count;
};
#endif   /* PELOTON_H */

