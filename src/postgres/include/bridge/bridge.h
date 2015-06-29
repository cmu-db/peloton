/**
 * @brief Header for postgres bridge.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include "c.h"

//===--------------------------------------------------------------------===//
//  Bridge for accessing Postgres Catalog
//===--------------------------------------------------------------------===//

#ifdef __cplusplus
extern "C" {
#endif

char* GetRelationName(Oid relation_id);

int GetNumberOfAttributes(Oid relation_id);

float GetNumberOfTuples(Oid relation_id);

Oid GetCurrentDatabaseOid(void);

void SetNumberOfTuples(Oid relation_id, float num_of_tuples);

void GetDatabaseList(void);

void GetTableList(void);

void GetPublicTableList(void);

bool IsThisTableExist(const char* table_name);

bool InitPeloton(const char* db_name);

Oid GetRelationOidFromRelationName(const char *table_name);

void SetUserTableStats(Oid relation_id);

void FunctionTest(void);

#ifdef __cplusplus
}
#endif
