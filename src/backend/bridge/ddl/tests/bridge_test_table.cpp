//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// bridge_test_table.cpp
//
// Identification: src/backend/bridge/ddl/tests/bridge_test_table.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "bridge_test.h"

#include "backend/bridge/ddl/bridge.h"
#include "backend/bridge/ddl/ddl.h"
#include "backend/bridge/ddl/ddl_table.h"
#include "backend/catalog/manager.h"
#include "backend/common/exception.h"
#include "backend/storage/database.h"

namespace peloton {
namespace bridge {

/**
 * @brief Test "DDLTable" function
 */
void BridgeTest::DDL_Table_TEST() {
  DDL_CreateTable_TEST_WITH_INVALID_OID();

  DDL_CreateTable_TEST_WITH_COLUMNS();
}

/**
 * @brief CreateTable with INVALID OID
 *        It MUST return false
 */
void BridgeTest::DDL_CreateTable_TEST_WITH_INVALID_OID() {
  // Empty Column
  std::vector<catalog::Column> columns;

  // Table name and oid
  std::string table_name = "test_table_invalid_oid";
  oid_t table_oid = INVALID_OID;

  // Create a table
  bool status = DDLTable::CreateTable(table_oid, table_name, columns);

  // CHECK :: status must be false
  if(status != false)
    throw CatalogException("Could create table");

  LOG_INFO(":::::: %s DONE\n", __func__);
}

/**
 * @brief Create a table with simple Columns
 */
void BridgeTest::DDL_CreateTable_TEST_WITH_COLUMNS() {
  auto &manager = catalog::Manager::GetInstance();
  storage::Database *db =
      manager.GetDatabaseWithOid(Bridge::GetCurrentDatabaseOid());
  assert(db);

  // Get the simple columns
  std::vector<catalog::Column> columns = CreateSimpleColumns();
  assert(columns.size() > 0);

  // Table name and oid
  std::string table_name = "test_table_basic_columns";
  oid_t table_oid = 20001;

  // Create a table
  bool status = DDLTable::CreateTable(table_oid, table_name, columns);
  if(status == false)
    throw CatalogException("Could not create table");

  // Get the table pointer
  storage::DataTable *table = db->GetTableWithOid(table_oid);

  // Check the table name and oid
  if(strcmp((table->GetName()).c_str(), table_name.c_str()) != 0)
    throw CatalogException("Name check failed");

  if(table->GetOid() != table_oid)
    throw CatalogException("Oid check failed");

  // Get the table's schema to get a column
  catalog::Schema *schema = table->GetSchema();
  std::cout << (*schema);

  // Check the first column' name, length and value type
  catalog::Column column = schema->GetColumn(0);
  if(CheckColumn(column, "id", 4, VALUE_TYPE_INTEGER) == false)
    throw CatalogException("Column check failed");

  // Check the second column' name, length and value type
  column = schema->GetColumn(1);
  if(CheckColumn(column, "name", 68, VALUE_TYPE_VARCHAR) == false)
    throw CatalogException("Column check failed");

  // Check the third column' name, length and value type
  column = schema->GetColumn(2);
  if(CheckColumn(column, "time", 8, VALUE_TYPE_TIMESTAMP) == false)
    throw CatalogException("Column check failed");

  // Check the fourth column' name, length and value type
  column = schema->GetColumn(3);
  if(CheckColumn(column, "salary", 8, VALUE_TYPE_DOUBLE) == false)
    throw CatalogException("Column check failed");

  // Drop the table
  status = DDLTable::DropTable(table_oid);
  if(status == false)
    throw CatalogException("Drop table failed");

  LOG_INFO(":::::: %s DONE\n", __func__);
}

}  // End bridge namespace
}  // End peloton namespace
