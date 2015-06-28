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

/* ----------
 * The types of backend -> peloton messages
 * ----------
 */
typedef enum PelotonMsgType
{
  PELOTON_MTYPE_DUMMY,
  PELOTON_MTYPE_DEADLOCK
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
 * Peloton_MsgDummy        A dummy message, ignored by the collector
 * ----------
 */
typedef struct Peloton_MsgDummy
{
  Peloton_MsgHdr m_hdr;
} Peloton_MsgDummy;

/* ----------
 * Peloton_MsgDeadlock     Sent by the backend to tell the collector
 *                about a deadlock that occurred.
 * ----------
 */
typedef struct Peloton_MsgDeadlock
{
  Peloton_MsgHdr m_hdr;
  Oid     m_databaseid;
} Peloton_MsgDeadlock;


/* ----------
 * Peloton_Msg         Union over all possible messages.
 * ----------
 */
typedef union Peloton_Msg
{
  Peloton_MsgHdr msg_hdr;
  Peloton_MsgDummy msg_dummy;
  Peloton_MsgDeadlock msg_deadlock;
} Peloton_Msg;

/* ----------
 * Functions called from postmaster
 * ----------
 */

extern void peloton_init(void);
extern int  peloton_start(void);

#endif   /* PELOTON_H */

