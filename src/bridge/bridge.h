/**
 * @brief Header for postgres bridge.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

//===--------------------------------------------------------------------===//
//  Bridge for managing Postgres
//===--------------------------------------------------------------------===//

char* GetRelationName(Oid relation_id);

int GetNumberOfAttributes(Oid  relation_id);

float GetNumberOfTuples(Oid relation_id);

void SetNumberOfTuples(Oid relation_id, float num_of_tuples);

void GetDatabaseList(void);

void GetTableList(void);

void SetUserTableStats(Oid relation_id);
