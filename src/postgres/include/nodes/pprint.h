/*
 * pprint.h
 *
 *	Copyright(c) 2015 CMU
 *  Created on: Jun 18, 2015
 *      Author: Ming Fang
 */

#pragma once

#include "nodes/execnodes.h"
#include "executor/execdesc.h"

extern void printQueryDesc(const QueryDesc *queryDesc);
extern void printPlanStateTree(const PlanState *planstate);

