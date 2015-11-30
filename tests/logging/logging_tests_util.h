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

  static bool PrepareLogFile(LoggingType logging_type,
                                std::string log_file = "peloton.log");

  //===--------------------------------------------------------------------===//
  // CHECK RECOVERY
  //===--------------------------------------------------------------------===//

  static void ResetSystem(void);

  static void CheckRecovery(LoggingType logging_type,
                                std::string log_file = "peloton.log");

  //===--------------------------------------------------------------------===//
  // Configuration
  //===--------------------------------------------------------------------===//

  static void ParseArguments(int argc, char* argv[]);

  class logging_test_configuration {
   public:

    // # of tuples
    int tuple_count;

    // # of backends (i.e. backend loggers)
    int backend_count;

    // tuple size
    int tuple_size;

    // check if the count matches after recovery
    bool check_tuple_count;

    // SUSPEND_COMMIT: suspend final step in transaction commit,
    // so that it only get committed during recovery
    bool suspend_commit;

    // enable file backend ?
    bool file_backend;

   };

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

  static std::vector<storage::Tuple*> CreateTuples(catalog::Schema* schema, oid_t num_of_tuples);

  static void DropDatabaseAndTable(oid_t db_oid, oid_t table_oid);

  static void DropDatabase(oid_t db_oid);

  static void CheckTupleCount(oid_t db_oid, oid_t table_oid, oid_t expected);

};

}  // End test namespace
}  // End peloton namespace
