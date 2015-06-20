/*
 * pprint.h
 *
 *	Copyright(c) 2015 CMU
 *  Created on: Jun 18, 2015
 *      Author: Ming Fang
 */

#pragma once

void printPostgresQueryDesc(const QueryDesc *queryDesc);
void printPostgresPlanStateTree(const PlanState *planstate);

