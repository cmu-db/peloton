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

  //===--------------------------------------------------------------------===//
  // PREPARE LOG FILE
  //===--------------------------------------------------------------------===//

  static bool PrepareLogFile(LoggingType logging_type);

  // Truncate the log file for Peloton Recovery Test
  static void TruncateLogFile(std::string file_name);

  //===--------------------------------------------------------------------===//
  // CHECK RECOVERY
  //===--------------------------------------------------------------------===//

  static void CheckAriesRecovery(void);

  static void CheckPelotonRecovery(void);

  static void CheckRollBack(oid_t db_oid, oid_t table_oid);

  static void CheckTupleCount(oid_t db_oid, oid_t table_oid);

private:

  //===--------------------------------------------------------------------===//
  // WRITING LOG RECORD
  //===--------------------------------------------------------------------===//

  static void BuildLog(oid_t db_oid, oid_t table_oid, LoggingType logging_type);

  static void RunBackends(storage::DataTable* table);

  static std::vector<ItemPointer> InsertTuples(storage::DataTable* table, bool committed);

  static void DeleteTuples(storage::DataTable* table, ItemPointer location, bool committed);

  static void UpdateTuples(storage::DataTable* table, ItemPointer location, bool committed);
  
  //===--------------------------------------------------------------------===//
  // Utility functions
  //===--------------------------------------------------------------------===//

  static void CreateDatabase(oid_t db_oid);

  static void CreateDatabaseAndTable(oid_t db_oid, oid_t table_oid);

  static storage::DataTable* CreateSimpleTable(oid_t db_oid, oid_t table_oid);

  static std::vector<catalog::Column> CreateSchema(void);

  static std::vector<storage::Tuple*> GetTuple(catalog::Schema* schema, oid_t num_of_tuples);

  static void DropDatabaseAndTable(oid_t db_oid, oid_t table_oid);

  static void DropDatabase(oid_t db_oid);

};

}  // End test namespace
}  // End peloton namespace
