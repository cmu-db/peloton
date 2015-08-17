#pragma once

#include <vector>

#include "backend/common/types.h"

namespace peloton {

namespace catalog {
class Column;
}

namespace test {

class LoggingTestsUtil{
public:
  static void CreateDatabase(oid_t db_oid);

  static void DropDatabase(oid_t db_oid);

  static void CreateTable(oid_t db_oid,
                          oid_t table_oid,
                          std::string table_name);

  static void DropTable(oid_t db_oid,
                        oid_t table_oid);

  static std::vector<catalog::Column> CreateSimpleColumns(void);
};
}  // End test namespace
}  // End peloton namespace
