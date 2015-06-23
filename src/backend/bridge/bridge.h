/**
 * @brief Header for postgres bridge.
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

//===--------------------------------------------------------------------===//
//  Bridge for managing Postgres
//===--------------------------------------------------------------------===//

#ifdef __cplusplus
extern "C" {
#endif

char* GetRelationName(unsigned int relation_id);

int GetNumberOfAttributes(unsigned int  relation_id);

float GetNumberOfTuples(unsigned int relation_id);

int GetCurrentDatabaseOid(void);

void SetNumberOfTuples(unsigned int relation_id, float num_of_tuples);

void GetDatabaseList(void);

void GetTableList(void);

void GetPublicTableList(void);

bool IsThisTableExist(const char* table_name);

bool InitPeloton(void);

void SetUserTableStats(unsigned int relation_id);

void FunctionTest(void);

#ifdef __cplusplus
}
#endif
