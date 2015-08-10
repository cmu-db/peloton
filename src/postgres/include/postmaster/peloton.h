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

#include "libpq/libpq-be.h"
#include "nodes/execnodes.h"
#include "utils/memutils.h"

/* ----------
 * The types of backend -> peloton messages
 * ----------
 */
typedef enum PelotonMsgType
{
  PELOTON_MTYPE_INVALID,    // Invalid message type
  PELOTON_MTYPE_DUMMY,      // Dummy message type
  PELOTON_MTYPE_DDL,        // DDL information to Peloton
  PELOTON_MTYPE_DML,        // DML information to Peloton
  PELOTON_MTYPE_BOOTSTRAP,  // BOOTSTRAP information to Peloton
  PELOTON_MTYPE_REPLY       // Reply message from Peloton to Backend
} PelotonMsgType;

/* ------------------------------------------------------------
 * Message formats follow
 * ------------------------------------------------------------
 */

/* ----------
 * Peloton_MsgHdr        The common message header
 * ----------
 */
typedef struct Peloton_MsgHdr
{
  PelotonMsgType m_type;
  int     m_size;
  BackendId m_backend_id;
  Oid   m_dbid;
  TransactionId m_txn_id;
} Peloton_MsgHdr;

/* ----------
 * Stats Sent by the peloton to share the status with backend.
 * ----------
 */
typedef struct dirty_index_info{
  Oid index_oid;
  float number_of_tuples;
}dirty_index_info;

typedef struct dirty_table_info{
  Oid table_oid;
  float number_of_tuples;
  dirty_index_info** dirty_indexes;
  Oid dirty_index_count;
}dirty_table_info;

/* ----------
 * Peloton_Status     Sent by the peloton to share the status with backend.
 * ----------
 */
typedef struct Peloton_Status
{
  peloton::Result m_result;
  List *m_result_slots;
  int m_status;
  dirty_table_info** m_dirty_tables;
  int m_dirty_count;
} Peloton_Status;

/* ----------
 * Space available in a message.  This will keep the UDP packets below 1K,
 * which should fit unfragmented into the MTU of the loopback interface.
 * (Larger values of PELOTON_MAX_MSG_SIZE would work for that on most
 * platforms, but we're being conservative here.)
 * ----------
 */
#define PELOTON_MAX_MSG_SIZE 1000
#define PELOTON_MSG_PAYLOAD  (PELOTON_MAX_MSG_SIZE - sizeof(Peloton_MsgHdr))

/* ----------
 * Peloton_MsgDummy        A dummy message, ignored by peloton.
 * ----------
 */
typedef struct Peloton_MsgDummy
{
  Peloton_MsgHdr m_hdr;
} Peloton_MsgDummy;

/* ----------
 * Peloton_MsgDML     Sent by the backend to share the plan to peloton.
 * ----------
 */
typedef struct Peloton_MsgDML
{
  Peloton_MsgHdr m_hdr;
  Peloton_Status  *m_status;
  Plan *m_plan;
  ParamListInfo m_param_list;
  TupleDesc m_tuple_desc;
  peloton::planner::AbstractPlanState *m_peloton_planstate;
} Peloton_MsgDML;

/* ----------
 * Peloton_MsgDDL     Sent by the backend to share the plan to peloton.
 * ----------
 */
typedef struct Peloton_MsgDDL
{
  Peloton_MsgHdr m_hdr;
  Peloton_Status  *m_status;
  Node *m_parsetree;
} Peloton_MsgDDL;

/* ----------
 * Peloton_MsgBootstrap  Sent by the backend to share the raw database to peloton.
 * ----------
 */
typedef struct Peloton_MsgBootstrap
{
  Peloton_MsgHdr m_hdr;
  Peloton_Status  *m_status;
  peloton::bridge::raw_database_info *m_raw_database;
} Peloton_MsgBootstrap;

/* ----------
 * Peloton_Msg         Union over all possible messages.
 * ----------
 */
typedef union Peloton_Msg
{
  Peloton_MsgHdr msg_hdr;
  Peloton_MsgDummy msg_dummy;
  Peloton_MsgDDL msg_ddl;
  Peloton_MsgDML msg_dml;
} Peloton_Msg;

/* Status inquiry functions */
extern bool IsPelotonProcess(void);

/* ----------
 * Functions called from postmaster
 * ----------
 */

extern bool IsPelotonQuery(List *relationOids);

extern void peloton_init(void);
extern int  peloton_start(void);

/* ----------
 * Functions called from execMain and utility
 * ----------
 */

extern void peloton_send_dml(Peloton_Status  *status,
                             PlanState *planstate,
                             TupleDesc tuple_desc);

extern void peloton_send_ddl(Peloton_Status  *status,
                             Node *parsetree);

extern void peloton_send_bootstrap(Peloton_Status *status);

extern Peloton_Status *peloton_create_status();

extern void peloton_process_status(Peloton_Status *status);

extern void peloton_destroy_status(Peloton_Status *status);

#endif   /* PELOTON_H */

