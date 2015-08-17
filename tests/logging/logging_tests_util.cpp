#include "backend/bridge/ddl/ddl_database.h"
#include "backend/bridge/ddl/ddl_table.h"
#include "backend/storage/table_factory.h"
#include "backend/storage/database.h"
#include "backend/storage/data_table.h"
#include "logging/logging_tests_util.h"

namespace peloton {
namespace test {

void LoggingTestsUtil::CreateDatabase(oid_t db_oid){
  peloton::bridge::DDLDatabase::CreateDatabase(db_oid);
}

void LoggingTestsUtil::DropDatabase(oid_t db_oid){
  peloton::bridge::DDLDatabase::DropDatabase(db_oid);
}

void LoggingTestsUtil::CreateTable(oid_t db_oid, 
                                   oid_t table_oid,
                                   std::string table_name){
  auto column_infos = LoggingTestsUtil::CreateSimpleColumns();

  // Construct our schema from vector of ColumnInfo
  auto schema = new catalog::Schema(column_infos);

  storage::DataTable *table = storage::TableFactory::GetDataTable( db_oid, table_oid, schema, table_name);

  auto &manager = catalog::Manager::GetInstance();
  storage::Database *db = manager.GetDatabaseWithOid(db_oid);
  db->AddTable(table);
}

void LoggingTestsUtil::DropTable(oid_t db_oid,
                                 oid_t table_oid){
  auto &manager = catalog::Manager::GetInstance();
  storage::Database *db = manager.GetDatabaseWithOid(db_oid);
  db->DropTableWithOid(table_oid);
}

/**
 * @brief Create a simple Column just for convenience
 * @return the vector of Column
 */
std::vector<catalog::Column> LoggingTestsUtil::CreateSimpleColumns() {
  // Column
  std::vector<catalog::Column> columns;

  catalog::Column column1(VALUE_TYPE_INTEGER, 4, "id");
  catalog::Column column2(VALUE_TYPE_VARCHAR, 68, "name");
  catalog::Column column3(VALUE_TYPE_TIMESTAMP, 8, "time");
  catalog::Column column4(VALUE_TYPE_DOUBLE, 8, "salary");

  columns.push_back(column1);
  columns.push_back(column2);
  columns.push_back(column3);
  columns.push_back(column4);

  return columns;
}

}  // End test namespace
}  // End peloton namespace
