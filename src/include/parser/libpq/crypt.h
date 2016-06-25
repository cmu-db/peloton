//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// crypt.h
//
// Identification: src/include/parser/libpq/crypt.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


/*-------------------------------------------------------------------------
 *
 * crypt.h
 *	  Interface to libpq/crypt.c
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/libpq/crypt.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_CRYPT_H
#define PG_CRYPT_H

#include "parser/libpq/libpq-be.h"

extern int md5_crypt_verify(const Port *port, const char *role,
				 char *client_pass, char **logdetail);

#endif
