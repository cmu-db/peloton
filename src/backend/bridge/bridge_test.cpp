/*-------------------------------------------------------------------------
 *
 * bridge_test.cpp
 * file description
 *
 * Copyright(c) 2015, CMU

 * /peloton/tests/bridge/bridge_test.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "bridge_test.h"

#include "backend/bridge/bridge.h"
#include "backend/bridge/ddl.h"
#include "backend/bridge/ddl_table.h"
#include "backend/bridge/ddl_index.h"
#include "backend/catalog/manager.h"
#include "backend/storage/database.h"

namespace peloton {
namespace bridge {

/**
 * @brief Test "DDL::CreateTable" function
 *        with various test cases
 */
void BridgeTest::DDL_CreateTable_TEST() {

  // Create the new storage database and add it to the manager
  storage::Database* db = new storage::Database( Bridge::GetCurrentDatabaseOid());
  auto& manager = catalog::Manager::GetInstance();
  manager.AddDatabase(db);


  DDL_CreateTable_TEST_INVALID_OID();

  DDL_CreateTable_TEST_COLUMNS();

  DDL_CreateTable_TEST_COLUMN_CONSTRAINTS();

  // TODO::
  //DDL_CreateTable_TEST_TABLE_CONSTRAINTS();

}

/**
 * @brief CreateTable with INVALID OID
 *        It MUST return false 
 */
void BridgeTest::DDL_CreateTable_TEST_INVALID_OID() {

  // Empty Column
  std::vector<catalog::Column> columns;

  // Table name and oid
  std::string table_name = "test_table_invalid_oid";
  oid_t table_oid = INVALID_OID;

  // Create a table
  bool status = DDLTable::CreateTable(table_oid, table_name, columns);

  // CHECK :: status must be false
  assert(status == false);

  std::cout << ":::::: " << __func__ << " DONE\n";
}

/**
 * @brief Create a table with simple Columns
 */
void BridgeTest::DDL_CreateTable_TEST_COLUMNS() {

  auto& manager = catalog::Manager::GetInstance();
  storage::Database* db = manager.GetDatabaseWithOid(Bridge::GetCurrentDatabaseOid());
  assert( db );

  // Get the simple columns
  std::vector<catalog::Column> columns = CreateSimpleColumns();
  assert( columns.size() > 0 );

  // Table name and oid
  std::string table_name = "test_table_basic_columns";
  oid_t table_oid = 20001;

  // Create a table
  bool status = DDLTable::CreateTable(table_oid, table_name, columns);
  assert(status);

  // Get the table pointer
  storage::DataTable* table = db->GetTableWithOid(table_oid);

  // Check the table name and oid
  assert(strcmp((table->GetName()).c_str(), table_name.c_str()) == 0);
  assert(table->GetOid() == table_oid);

  // Get the table's schema to get a column 
  catalog::Schema* schema = table->GetSchema();
  std::cout <<(*schema);

  // Check the first column' name, length and value type
  catalog::Column column = schema->GetColumn(0);
  assert( CheckColumn( column, "id", 4, VALUE_TYPE_INTEGER ));

  // Check the second column' name, length and value type
  column = schema->GetColumn(1);
  assert( CheckColumn( column, "name", 68, VALUE_TYPE_VARCHAR ));

  // Check the third column' name, length and value type
  column = schema->GetColumn(2);
  assert( CheckColumn( column, "time", 8, VALUE_TYPE_TIMESTAMP ));

  // Check the fourth column' name, length and value type
  column = schema->GetColumn(3);
  assert( CheckColumn( column, "salary", 8, VALUE_TYPE_DOUBLE ));

  std::cout << ":::::: " << __func__ << " DONE\n";
}

/**
 * @brief Create a table with simple columns that contain
 *        column-level constraints such as single column 
 *        primary key, unique, and reference table 
 */
void BridgeTest::DDL_CreateTable_TEST_COLUMN_CONSTRAINTS() {

  auto& manager = catalog::Manager::GetInstance();
  storage::Database* db = manager.GetDatabaseWithOid(Bridge::GetCurrentDatabaseOid());

  // Get the simple columns
  std::vector<catalog::Column> columns = CreateSimpleColumns();

  // Table name and oid
  std::string table_name = "test_table_column_constraint";
  oid_t table_oid = 20002;

  // Create a table
  bool status = DDLTable::CreateTable(table_oid, table_name, columns);
  assert(status);

  // Get the table pointer and schema
  storage::DataTable* table = db->GetTableWithOid(table_oid);
  catalog::Schema *schema = table->GetSchema();

  // Create the constrains
  catalog::Constraint notnull_constraint(CONSTRAINT_TYPE_NOTNULL);

  // Add one constraint to the one column
  schema->AddConstraint("id", notnull_constraint);

  // Create a primary key index and added primary key constraint to the 'name' column
  CreateSamplePrimaryKeyIndex(table_name, 30001);

  // Create a unique index and added unique constraint to the 'time' column
  CreateSampleUniqueIndex(table_name, 30002);

  // Create a reference table and foreign key constraint and added unique constraint to the 'salary' column
  std::string pktable_name = "pktable";
  oid_t pktable_oid = 20003;
  CreateSampleForeignKey(pktable_oid, pktable_name, columns, table_oid);

  // Check the first column's constraint 
  catalog::Column column = schema->GetColumn(0);
  CheckColumnWithConstraint( column, CONSTRAINT_TYPE_NOTNULL, "", 1);

  // Check the second column's constraint and index
  column = schema->GetColumn(1);
  CheckColumnWithConstraint( column, CONSTRAINT_TYPE_PRIMARY, table_name+"_pkey", 1);
  index::Index *index = table->GetIndexWithOid(30001);
  CheckIndex(index, table_name+"_pkey", 1, INDEX_TYPE_BTREE_MULTIMAP, INDEX_CONSTRAINT_TYPE_PRIMARY_KEY, true);

  // Check the third column's constraint and index
  column = schema->GetColumn(2);
  CheckColumnWithConstraint( column, CONSTRAINT_TYPE_UNIQUE, table_name+"_key", 1);
  index = table->GetIndexWithOid(30002);
  CheckIndex(index, table_name+"_key", 1, INDEX_TYPE_BTREE_MULTIMAP, INDEX_CONSTRAINT_TYPE_UNIQUE, true);

  // Check the fourth column's constraint and foreign key
  column = schema->GetColumn(3);
  CheckColumnWithConstraint( column, CONSTRAINT_TYPE_FOREIGN, "THIS_IS_FOREIGN_CONSTRAINT", 1, 0);
  catalog::ForeignKey *pktable = table->GetForeignKey(0);
  CheckForeignKey( pktable, pktable_oid, "THIS_IS_FOREIGN_CONSTRAINT", 1, 1, 'r', 'c' );

  std::cout << ":::::: " << __func__ << " DONE\n";
}

void BridgeTest::RunTests() {
  std::cout<< ":::::::::::::  TEST CASES START :::::::::::::\n";
  DDL_CreateTable_TEST();
  std::cout<< ":::::::::::::  TEST CASES END   :::::::::::::\n";
}

} // End bridge namespace
} // End peloton namespace

