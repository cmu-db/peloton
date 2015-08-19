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

  static void PrepareLogFile(void);

  static void CheckTupleAfterRecovery(void);

private:

  static void CreateDatabaseAndTable(oid_t db_oid, oid_t table_oid);

  static void DropDatabaseAndTable(oid_t db_oid, oid_t table_oid);

  static void CheckTuples(oid_t db_oid, oid_t table_oid);

  static void CheckNextOid(void);

  static void WritingSimpleLog(oid_t db_oid, oid_t table_oid);

  static void CreateDatabase(oid_t db_oid);

  static void DropDatabase(oid_t db_oid);

  static storage::DataTable* CreateSimpleTable(oid_t db_oid, oid_t table_oid);

  static std::vector<catalog::Column> CreateSimpleColumns(void);

  static std::vector<storage::Tuple*> CreateSimpleTuples(catalog::Schema* schema);

  static void InsertTuples(storage::DataTable* table);

};

}  // End test namespace
}  // End peloton namespace
