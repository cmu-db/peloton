//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// fork_process.h
//
// Identification: src/include/parser/postmaster/fork_process.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


/*-------------------------------------------------------------------------
 *
 * fork_process.h
 *	  Exports from postmaster/fork_process.c.
 *
 * Copyright (c) 1996-2015, PostgreSQL Global Development Group
 *
 * src/include/postmaster/fork_process.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef FORK_PROCESS_H
#define FORK_PROCESS_H

#include <thread>

extern pid_t fork_process(void);

#endif   /* FORK_PROCESS_H */
