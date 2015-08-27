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

  /////////////////////////////////////////////////////////////////////
  // PREPARE LOG FILE
  /////////////////////////////////////////////////////////////////////

  static bool PrepareLogFile(LoggingType logging_type);

  static size_t GetLogFileSize(std::string file_name);

  /////////////////////////////////////////////////////////////////////
  // CHECK RECOVERY
  /////////////////////////////////////////////////////////////////////

  static void CheckAriesRecovery(void);

  static void CheckPelotonRecovery(void);

  static void CheckTupleCount(oid_t db_oid, oid_t table_oid);

private:

  /////////////////////////////////////////////////////////////////////
  // WRITING LOG RECORD
  /////////////////////////////////////////////////////////////////////

  static void WritingSimpleLog(oid_t db_oid, oid_t table_oid, LoggingType logging_type);

  static void ParallelWriting(storage::DataTable* table);

  static std::vector<ItemPointer> InsertTuples(storage::DataTable* table, bool committed);

  static void DeleteTuples(storage::DataTable* table, ItemPointer location, bool committed);

  static void UpdateTuples(storage::DataTable* table, ItemPointer location, bool committed);
  
  /////////////////////////////////////////////////////////////////////
  // CREATE PELOTON OBJECT 
  /////////////////////////////////////////////////////////////////////

  static void CreateDatabase(oid_t db_oid);

  static void CreateDatabaseAndTable(oid_t db_oid, oid_t table_oid);

  static storage::DataTable* CreateSimpleTable(oid_t db_oid, oid_t table_oid);

  static std::vector<catalog::Column> CreateSimpleColumns(void);

  static std::vector<storage::Tuple*> CreateSimpleTuple(catalog::Schema* schema, oid_t num_of_tuples);

  /////////////////////////////////////////////////////////////////////
  // DROP PELOTON OBJECT
  /////////////////////////////////////////////////////////////////////

  static void DropDatabaseAndTable(oid_t db_oid, oid_t table_oid);

  static void DropDatabase(oid_t db_oid);

};

}  // End test namespace
}  // End peloton namespace
