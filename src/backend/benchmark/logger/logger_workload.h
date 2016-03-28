//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// logger_workload.h
//
// Identification: src/backend/benchmark/logger/logger_workload.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/benchmark/logger/logger_configuration.h"

namespace peloton {

namespace catalog {
class Column;
class Schema;
}

namespace storage {
class Tuple;
class DataTable;
}

namespace benchmark {
namespace logger {

extern configuration state;

//===--------------------------------------------------------------------===//
// PREPARE LOG FILE
//===--------------------------------------------------------------------===//

bool PrepareLogFile(std::string file_name);

//===--------------------------------------------------------------------===//
// CHECK RECOVERY
//===--------------------------------------------------------------------===//

void ResetSystem(void);

void DoRecovery(std::string file_name);

//===--------------------------------------------------------------------===//
// WRITING LOG RECORD
//===--------------------------------------------------------------------===//

void BuildLog(oid_t db_oid, oid_t table_oid);

void RunBackends(storage::DataTable* table,
                 const std::vector<storage::Tuple*>& tuples);

std::vector<ItemPointer> InsertTuples(
    storage::DataTable* table, const std::vector<storage::Tuple*>& tuples,
    bool committed);

void DeleteTuples(storage::DataTable* table,
                  const std::vector<ItemPointer>& locations, bool committed);

std::vector<ItemPointer> UpdateTuples(
    storage::DataTable* table, const std::vector<ItemPointer>& locations,
    const std::vector<storage::Tuple*>& tuples, bool committed);

//===--------------------------------------------------------------------===//
// Utility functions
//===--------------------------------------------------------------------===//

void CreateDatabase(oid_t db_oid);

void CreateDatabaseAndTable(oid_t db_oid, oid_t table_oid);

storage::DataTable* CreateUserTable(oid_t db_oid, oid_t table_oid);

std::vector<catalog::Column> CreateSchema(void);

std::vector<storage::Tuple*> CreateTuples(catalog::Schema* schema,
                                          oid_t num_of_tuples,
                                          VarlenPool* pool);

void DropDatabaseAndTable(oid_t db_oid, oid_t table_oid);

void DropDatabase(oid_t db_oid);

void CheckTupleCount(oid_t db_oid, oid_t table_oid, oid_t expected);

}  // namespace logger
}  // namespace benchmark
}  // namespace peloton
