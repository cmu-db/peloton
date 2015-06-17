/*
 * bridge.h
 *
 *  Created on: Jun 11, 2015
 *      Author: Jinwoong Kim
 */

#pragma once

//#include "postgres.h"

char* GetRelationName(unsigned int relation_id);

int GetNumberOfAttributes(unsigned int relation_id);

float GetNumberOfTuples(unsigned int relation_id);

void GetDatabaseList(void);

void GetTableList(void);


