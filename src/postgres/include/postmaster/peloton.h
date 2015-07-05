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
  PELOTON_MTYPE_DDL,        // DDL information
  PELOTON_MTYPE_DML         // DML information
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
} Peloton_MsgHdr;

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
  PlanState *m_planstate;
  bool m_sendTuples;
  DestReceiver *m_dest;
} Peloton_MsgDML;

/* ----------
 * Peloton_MsgDDL     Sent by the backend to share the plan to peloton.
 * ----------
 */
typedef struct Peloton_MsgDDL
{
  Peloton_MsgHdr m_hdr;
  Node *m_parsetree;
  char *m_queryString;
  MemoryContext m_top_transaction_context;
  MemoryContext m_cur_transaction_context;
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

extern void peloton_init(void);
extern int  peloton_start(void);

extern void peloton_send_ping(void);
extern void peloton_send_dml(PlanState *node, bool sendTuples, DestReceiver *dest);
extern void peloton_send_ddl(Node *parsetree, const char *queryString,
                             MemoryContext top_transaction_context,
                             MemoryContext cur_transaction_context);

#endif   /* PELOTON_H */

