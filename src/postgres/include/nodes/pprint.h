/*
 * pprint.h
 *
 *	Copyright(c) 2015 CMU
 *  Created on: Jun 18, 2015
 *      Author: Ming Fang
 */

#ifndef SRC_POSTGRES_INCLUDE_NODES_PPRINT_H_
#define SRC_POSTGRES_INCLUDE_NODES_PPRINT_H_
#include "postgres.h"
#include "executor/execdesc.h"


extern void PrintQueryDesc(const QueryDesc *queryDesc);
extern void PrintPlanStateTree(const PlanState *planstate);

#endif /* SRC_POSTGRES_INCLUDE_NODES_PPRINT_H_ */
