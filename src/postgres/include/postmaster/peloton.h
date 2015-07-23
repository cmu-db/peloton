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
  TransactionId m_txn_id;
  MemoryContext m_top_transaction_context;
  MemoryContext m_cur_transaction_context;
} Peloton_MsgHdr;

/* ----------
 * Peloton_Status     Sent by the peloton to share the status with backend.
 * ----------
 */
typedef struct Peloton_Status
{
  peloton::ResultType  m_code;
  List *m_result_slots;
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
  PlanState *m_planstate;
  TupleDesc m_tuple_desc;
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
  const char *m_queryString;
} Peloton_MsgDDL;

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

extern void peloton_send_ping(void);

/* ----------
 * Functions called from execMain and utility
 * ----------
 */

extern void peloton_send_dml(Peloton_Status  *status,
                             PlanState *node,
                             TupleDesc tuple_desc);

extern void peloton_send_ddl(Peloton_Status  *status,
                             Node *parsetree,
                             const char *queryString);

extern Peloton_Status *peloton_create_status();

extern void peloton_process_status(Peloton_Status *status);

extern void peloton_destroy_status(Peloton_Status *status);


#endif   /* PELOTON_H */

