/*-------------------------------------------------------------------------
 *
 * printtup.h
 *
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/printtup.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PRINTTUP_H
#define PRINTTUP_H

#include "utils/portal.h"

extern DestReceiver *printtup_create_DR(CommandDest dest);

extern void SetRemoteDestReceiverParams(DestReceiver *self, Portal portal);

extern void SendRowDescriptionMessage(TupleDesc typeinfo, List *targetlist,
						  int16 *formats);

extern void debugStartup(DestReceiver *self, int operation,
			 TupleDesc typeinfo, MemcachedState *mc_state = nullptr);
extern void debugtup(TupleTableSlot *slot, DestReceiver *self,
										 MemcachedState *mc_state = nullptr);

/* XXX these are really in executor/spi.c */
extern void spi_dest_startup(DestReceiver *self, int operation,
				 TupleDesc typeinfo,
														 MemcachedState *mc_state = nullptr);
extern void spi_printtup(TupleTableSlot *slot, DestReceiver *self,
												 MemcachedState *mc_state = nullptr);

extern void printtup(TupleTableSlot *slot, DestReceiver *self,
										 MemcachedState *mc_state = nullptr);

#endif   /* PRINTTUP_H */
