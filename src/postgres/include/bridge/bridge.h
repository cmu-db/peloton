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

#include "access/htup.h"

//===--------------------------------------------------------------------===//
// Getters
//===--------------------------------------------------------------------===//

HeapTuple GetPGClassTupleForRelationOid(Oid relation_id);

HeapTuple GetPGClassTupleForRelationName(const char *relation_name);

//===--------------------------------------------------------------------===//
// Oid <--> Name
//===--------------------------------------------------------------------===//

char* GetRelationName(Oid relation_id);

Oid GetRelationOid(const char *relation_name);

//===--------------------------------------------------------------------===//
// Catalog information
//===--------------------------------------------------------------------===//

int GetNumberOfAttributes(Oid relation_id);

float GetNumberOfTuples(Oid relation_id);

bool RelationExists(const char* relation_name);

Oid GetCurrentDatabaseOid(void);

//===--------------------------------------------------------------------===//
// Table Lists
//===--------------------------------------------------------------------===//

void GetDatabaseList(void);

void GetTableList(bool catalog_only);

//===--------------------------------------------------------------------===//
// Setters
//===--------------------------------------------------------------------===//

void SetNumberOfTuples(Oid relation_id, float num_of_tuples);

// Initialize

bool BootstrapPeloton(void);

#ifdef __cplusplus
}
#endif
