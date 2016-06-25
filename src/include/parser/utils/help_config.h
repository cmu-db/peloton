//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// help_config.h
//
// Identification: src/include/parser/utils/help_config.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


/*-------------------------------------------------------------------------
 *
 * help_config.h
 *		Interface to the --help-config option of main.c
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 *
 *	  src/include/utils/help_config.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HELP_CONFIG_H
#define HELP_CONFIG_H 1

extern void GucInfoMain(void) pg_attribute_noreturn();

#endif
