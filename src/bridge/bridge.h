/*
 * bridge.h
 *
 *  Created on: Jun 11, 2015
 *      Author: Jinwoong Kim
 */

#pragma once

char* GetRelationName(Oid relation_id);

int GetNumberOfAttributes(Oid  relation_id);

float GetNumberOfTuples(Oid relation_id);

void SetNumberOfTuples(Oid relation_id, float num_of_tuples);

void GetDatabaseList(void);

void GetTableList(void);

void Test1(unsigned int);
