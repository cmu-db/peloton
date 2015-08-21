#pragma once

#include <vector>

#include "backend/common/types.h"

namespace peloton {

namespace catalog {
class Column;
class Schema;
}
namespace storage {
class Tuple;
class DataTable;
}

namespace test {

class LoggingTestsUtil{

public:

  static bool PrepareLogFile(void);

  static void CheckTupleAfterRecovery(void);

private:

  static void WritingSimpleLog(oid_t db_oid, oid_t table_oid);

  static void CheckTuples(oid_t db_oid, oid_t table_oid);

  static void CreateDatabaseAndTable(oid_t db_oid, oid_t table_oid);

  static void CreateDatabase(oid_t db_oid);

  static storage::DataTable* CreateSimpleTable(oid_t db_oid, oid_t table_oid);

  static std::vector<catalog::Column> CreateSimpleColumns(void);

  static std::vector<storage::Tuple*> CreateSimpleTuple(catalog::Schema* schema, oid_t num_of_tuples);

  static void ParallelWriting(storage::DataTable* table);

  static std::vector<oid_t> InsertTuples(storage::DataTable* table, bool committed);

  static void DeleteTuples(storage::DataTable* table, oid_t tuple_slot, bool committed);

  static void UpdateTuples(storage::DataTable* table, oid_t tuple_slot, bool committed);

  static void DropDatabaseAndTable(oid_t db_oid, oid_t table_oid);

  static void DropDatabase(oid_t db_oid);

};

}  // End test namespace
}  // End peloton namespace
