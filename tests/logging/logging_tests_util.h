#pragma once

#include <vector>

#include "backend/common/types.h"
#include "backend/common/pool.h"

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

enum LoggingExperimentType {
  LOGGING_EXPERIMENT_TYPE_INVALID = 0,

  LOGGING_EXPERIMENT_TYPE_ACTIVE = 1,
  LOGGING_EXPERIMENT_TYPE_RECOVERY = 2,
  LOGGING_EXPERIMENT_TYPE_STORAGE = 3,
  LOGGING_EXPERIMENT_TYPE_WAIT = 4
};

class LoggingTestsUtil {
 public:
  //===--------------------------------------------------------------------===//
  // PREPARE LOG FILE
  //===--------------------------------------------------------------------===//

  static bool PrepareLogFile(std::string file_name);

  //===--------------------------------------------------------------------===//
  // CHECK RECOVERY
  //===--------------------------------------------------------------------===//

  static void ResetSystem(void);

  static void DoRecovery(std::string file_name);

  //===--------------------------------------------------------------------===//
  // Configuration
  //===--------------------------------------------------------------------===//

  static void ParseArguments(int argc, char* argv[]);

  class logging_test_configuration {
   public:
    // experiment type
    LoggingExperimentType experiment_type;

    // logging type
    LoggingType logging_type;

    // # of tuples
    int tuple_count;

    // # of backends (i.e. backend loggers)
    int backend_count;

    // # of columns in each tuple
    oid_t column_count;

    // check if the count matches after recovery
    bool check_tuple_count;

    // log file dir
    std::string log_file_dir;

    // size of the pmem file (in MB)
    size_t data_file_size;

    // frequency with which the logger flushes
    int64_t wait_timeout;
  };

 private:
  //===--------------------------------------------------------------------===//
  // WRITING LOG RECORD
  //===--------------------------------------------------------------------===//

  static void BuildLog(oid_t db_oid, oid_t table_oid);

  static void RunBackends(storage::DataTable* table,
                          const std::vector<storage::Tuple*>& tuples);

  static std::vector<ItemPointer> InsertTuples(
      storage::DataTable* table, const std::vector<storage::Tuple*>& tuples,
      bool committed);

  static void DeleteTuples(storage::DataTable* table,
                           const std::vector<ItemPointer>& locations,
                           bool committed);

  static std::vector<ItemPointer> UpdateTuples(
      storage::DataTable* table, const std::vector<ItemPointer>& locations,
      const std::vector<storage::Tuple*>& tuples, bool committed);

  //===--------------------------------------------------------------------===//
  // Utility functions
  //===--------------------------------------------------------------------===//

  static void CreateDatabase(oid_t db_oid);

  static void CreateDatabaseAndTable(oid_t db_oid, oid_t table_oid);

  static storage::DataTable* CreateUserTable(oid_t db_oid, oid_t table_oid);

  static std::vector<catalog::Column> CreateSchema(void);

  static std::vector<storage::Tuple*> CreateTuples(catalog::Schema* schema,
                                                   oid_t num_of_tuples,
                                                   VarlenPool* pool);

  static void DropDatabaseAndTable(oid_t db_oid, oid_t table_oid);

  static void DropDatabase(oid_t db_oid);

  static void CheckTupleCount(oid_t db_oid, oid_t table_oid, oid_t expected);
};

// configuration for testing
extern LoggingTestsUtil::logging_test_configuration state;

}  // End test namespace
}  // End peloton namespace
