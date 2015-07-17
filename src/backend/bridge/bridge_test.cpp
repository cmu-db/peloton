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
#include "backend/catalog/manager.h"
#include "backend/storage/database.h"

namespace peloton {
namespace test {

/**
 * @brief Test "DDL::CreateTable" function
 *        with various test cases
 */
void BridgeTest::DDL_CreateTable_TEST() {

  // Create the new storage database and add it to the manager
  storage::Database* db = new storage::Database( bridge::Bridge::GetCurrentDatabaseOid());
  auto& manager = catalog::Manager::GetInstance();
  manager.AddDatabase(db);


  DDL_CreateTable_TEST_INVALID_OID();

  DDL_CreateTable_TEST_COLUMNS();

  DDL_CreateTable_TEST_COLUMN_CONSTRAINTS();

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
  bool status = bridge::DDL::CreateTable(table_oid, table_name, columns);

  // CHECK :: status must be false
  assert(status == false);

  std::cout << ":::::: " << __func__ << " DONE\n";
}

/**
 * @brief Create a simple Column just for convenience
 * @return the vector of Column
 */
std::vector<catalog::Column> BridgeTest::CreateSimpleColumns() {
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

/**
 * @brief Create a table with simple Columns
 */
void BridgeTest::DDL_CreateTable_TEST_COLUMNS() {

  auto& manager = catalog::Manager::GetInstance();
  storage::Database* db = manager.GetDatabaseWithOid(bridge::Bridge::GetCurrentDatabaseOid());
  assert( db );

  // Get the simple columns
  std::vector<catalog::Column> columns = CreateSimpleColumns();
  assert( columns.size() > 0 );

  // Table name and oid
  std::string table_name = "test_table_basic_columns";
  oid_t table_oid = 20001;

  // Create a table
  bool status = bridge::DDL::CreateTable(table_oid, table_name, columns);
  assert(status);

  // Get the table pointer
  storage::DataTable* table = db->GetTableWithOid(table_oid);

  // TODO::CHECK :: Table name and oid
  assert(strcmp((table->GetName()).c_str(), table_name.c_str()) == 0);
  assert(table->GetOid() == table_oid);

  // Get the table's schema to get a column 
  catalog::Schema* schema = table->GetSchema();

  std::cout <<(*schema);

  // The first column
  catalog::Column column = schema->GetColumn(0);

  // TODO::CHECK ::  Column name, length, type
  assert(strcmp(column.GetName().c_str(), "id") == 0);
  assert(column.GetLength() == 4);
  assert(column.GetType() == VALUE_TYPE_INTEGER);

  // The second column
  column = schema->GetColumn(1);

  // TODO::CHECK ::  Column name, length, type
  assert(strcmp(column.GetName().c_str(), "name") == 0);
  assert(column.GetLength() == 68);
  assert(column.GetType() == VALUE_TYPE_VARCHAR);

  // The third column
  column = schema->GetColumn(2);

  // TODO::CHECK ::  Column name, length, type
  assert(strcmp(column.GetName().c_str(), "time") == 0);
  assert(column.GetLength() == 8);
  assert(column.GetType() == VALUE_TYPE_TIMESTAMP);

  // The fourth column
  column = schema->GetColumn(3);

  // TODO::CHECK ::  Column name, length, type
  assert(strcmp(column.GetName().c_str(), "salary") == 0);
  assert(column.GetLength() == 8);
  assert(column.GetType() == VALUE_TYPE_DOUBLE);

  std::cout << ":::::: " << __func__ << " DONE\n";
}

/**
 * @brief Create a table with simple columns that contain
 *        column-level constraints such as single column 
 *        primary key, unique, and reference table 
 */
void BridgeTest::DDL_CreateTable_TEST_COLUMN_CONSTRAINTS() {

  auto& manager = catalog::Manager::GetInstance();
  storage::Database* db = manager.GetDatabaseWithOid(bridge::Bridge::GetCurrentDatabaseOid());

  // Get the simple columns
  std::vector<catalog::Column> columns = CreateSimpleColumns();

  // Table name and oid
  std::string table_name = "test_table_column_constraint";
  oid_t table_oid = 20002;

  // Create a table
  bool status = bridge::DDL::CreateTable(table_oid, table_name, columns);
  assert(status);

  // Get the table pointer and schema
  storage::DataTable* table = db->GetTableWithOid(table_oid);
  catalog::Schema *schema = table->GetSchema();

  // Create the constrains
  catalog::Constraint notnull_constraint(CONSTRAINT_TYPE_NOTNULL);
  catalog::Constraint primary_key_constraint(CONSTRAINT_TYPE_PRIMARY, "THIS_IS_PRIMARY_KEY_CONSTRAINT");
  catalog::Constraint unique_constraint(CONSTRAINT_TYPE_UNIQUE, "THIS_IS_UNIQUE_CONSTRAINT");

  // Add one constraint to the one column
  schema->AddConstraint("id",    notnull_constraint);
  schema->AddConstraint("name",  primary_key_constraint);
  schema->AddConstraint("time",  unique_constraint);

  // Create a primary key index
  // NOTE:: It must be called automatically in active or bootstrap mode
  std::vector<std::string> key_column_names;
  key_column_names.push_back("name");
  bridge::DDL::IndexInfo* index_info = new bridge::DDL::IndexInfo("THIS_IS_PRIMARY_KEY_CONSTRAINT",
                                                                  30001,
                                                                  table_name,
                                                                  INDEX_TYPE_BTREE_MULTIMAP,
                                                                  INDEX_CONSTRAINT_TYPE_PRIMARY_KEY,
                                                                  true,
                                                                  key_column_names);
  status = bridge::DDL::CreateIndex(*index_info);
  assert(status);

  // Create a unique index
  key_column_names.clear();
  key_column_names.push_back("time");
  index_info = new bridge::DDL::IndexInfo("THIS_IS_UNIQUE_CONSTRAINT",
                                          30002,
                                          table_name,
                                          INDEX_TYPE_BTREE_MULTIMAP,
                                          INDEX_CONSTRAINT_TYPE_UNIQUE,
                                          true,
                                          key_column_names);

  status = bridge::DDL::CreateIndex(*index_info);
  assert(status);

  // Create a reference table that will be referenced by another table
  std::string pktable_name = "pktable";
  oid_t pktable_oid = 20003;
  status = bridge::DDL::CreateTable(pktable_oid, pktable_name, columns);
  assert(status);

  // Construct ForeignKey
  std::vector<std::string> pk_column_names;
  std::vector<std::string> fk_column_names;
  pk_column_names.push_back("name");
  fk_column_names.push_back("salary");
  std::vector<catalog::ForeignKey> foreign_keys;
  catalog::ForeignKey *foreign_key = new catalog::ForeignKey(pktable_oid,
                                                             pk_column_names,
                                                             fk_column_names,
                                                             'r',
                                                             'c',
                                                             "THIS_IS_FOREIGN_CONSTRAINT");
  foreign_keys.push_back(*foreign_key);


  // Current table ----> reference table
  status = peloton::bridge::DDL::SetReferenceTables(foreign_keys, table_oid);
  assert(status);

  // TODO::CHECK :: check only regarding constraint. Skip others, such as, column name, length, etc.

  // The first column
  // TODO::CHECK :: Not null constraint type 
  catalog::Column column = schema->GetColumn(0);
  std::vector<catalog::Constraint> constraint_infos = column.GetConstraints();
  assert(constraint_infos[0].GetType() == CONSTRAINT_TYPE_NOTNULL);


  // The second column
  // TODO::CHECK :: Primary key constraint type, name
  column = schema->GetColumn(1);
  constraint_infos = column.GetConstraints();
  assert(constraint_infos[0].GetType() == CONSTRAINT_TYPE_PRIMARY);
  assert(strcmp(constraint_infos[0].GetName().c_str(), "THIS_IS_PRIMARY_KEY_CONSTRAINT") == 0);

  // TODO::CHECK :: the primary key index name, type, etc.
  index::Index *index = table->GetIndexWithOid(30001);
  assert(strcmp(index->GetName().c_str(), "THIS_IS_PRIMARY_KEY_CONSTRAINT") == 0);
  assert(index->GetColumnCount() == 1);
  assert(index->GetIndexMethodType() == INDEX_TYPE_BTREE_MULTIMAP);
  assert(index->GetIndexType() == INDEX_CONSTRAINT_TYPE_PRIMARY_KEY);
  assert(index->HasUniqueKeys()  == true);


  // The third column
  // TODO::CHECK :: Unique constraint name, type, etc.
  column = schema->GetColumn(2);
  constraint_infos = column.GetConstraints();
  assert(constraint_infos[0].GetType() == CONSTRAINT_TYPE_UNIQUE);
  assert(strcmp(constraint_infos[0].GetName().c_str(), "THIS_IS_UNIQUE_CONSTRAINT") == 0);

  // TODO::Check  index for unique constraint
  index = table->GetIndexWithOid(30002);
  assert(strcmp(index->GetName().c_str(), "THIS_IS_UNIQUE_CONSTRAINT") == 0);
  assert(index->GetColumnCount() == 1);
  assert(index->GetIndexMethodType() == INDEX_TYPE_BTREE_MULTIMAP);
  assert(index->GetIndexType() == INDEX_CONSTRAINT_TYPE_UNIQUE);
  assert(index->HasUniqueKeys()  == true);

  // The fourth column
  column = schema->GetColumn(3);
  constraint_infos = column.GetConstraints();
  assert(constraint_infos.size() == 1);

  // TODO::CHECK::  reference table
  assert(constraint_infos[0].GetType() == CONSTRAINT_TYPE_FOREIGN);
  assert(strcmp(constraint_infos[0].GetName().c_str(), "THIS_IS_FOREIGN_CONSTRAINT") == 0);
  assert(constraint_infos[0].GetForeignKeyListOffset() == 0);

  // TODO::CHECK:: pktable name and oid
  catalog::ForeignKey *pktable = table->GetForeignKey(0);
  assert(pktable->GetSinkTableOid() == pktable_oid);
  assert(strcmp((pktable->GetConstraintName()).c_str(), "THIS_IS_FOREIGN_CONSTRAINT") == 0);

  // TODO::CHECK:: pktable size name, update/delete action
  pk_column_names.clear();
  fk_column_names.clear();
  pk_column_names  = pktable->GetPKColumnNames();
  fk_column_names  = pktable->GetFKColumnNames();
  assert(pk_column_names.size() == 1);
  assert(fk_column_names.size() == 1);
  assert(strcmp(pk_column_names[0].c_str(), "name") == 0);
  assert(strcmp(fk_column_names[0].c_str(), "salary") == 0);

  assert(pktable->GetUpdateAction() == 'r');
  assert(pktable->GetDeleteAction() == 'c');

  std::cout << ":::::: " << __func__ << " DONE\n";
}

void BridgeTest::RunTests() {
  std::cout<< ":::::::::::::  TEST CASES START :::::::::::::\n";
  DDL_CreateTable_TEST();
  std::cout<< ":::::::::::::  TEST CASES END   :::::::::::::\n";
}

} // End test namespace
} // End peloton namespace

