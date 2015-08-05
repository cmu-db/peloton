//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// bridge_test_mix.cpp
//
// Identification: src/backend/bridge/ddl/tests/bridge_test_mix.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "bridge_test.h"

#include "backend/bridge/ddl/bridge.h"
#include "backend/bridge/ddl/ddl.h"
#include "backend/bridge/ddl/ddl_table.h"
#include "backend/bridge/ddl/ddl_index.h"
#include "backend/catalog/manager.h"
#include "backend/storage/database.h"

namespace peloton {
namespace bridge {

/**
 * @brief Test many DDL functions together
 */
void BridgeTest::DDL_MIX_TEST() {
  DDL_MIX_TEST_1();

  DDL_MIX_TEST_2();
}

/**
 * @brief Create a table with simple columns that contain
 *        column-level constraints such as single column
 *        primary key, unique, and reference table
 */
void BridgeTest::DDL_MIX_TEST_1() {
  auto &manager = catalog::Manager::GetInstance();
  storage::Database *db =
      manager.GetDatabaseWithOid(Bridge::GetCurrentDatabaseOid());

  // Get the simple columns
  std::vector<catalog::Column> columns = CreateSimpleColumns();

  // Table name and oid
  std::string table_name = "test_table_column_constraint";
  oid_t table_oid = 50001;

  // Create a table
  bool status = DDLTable::CreateTable(table_oid, table_name, columns);
  assert(status);

  // Get the table pointer and schema
  storage::DataTable *table = db->GetTableWithOid(table_oid);
  catalog::Schema *schema = table->GetSchema();

  // Create the constrains
  catalog::Constraint notnull_constraint(CONSTRAINT_TYPE_NOTNULL);

  // Add one constraint to the one column
  schema->AddConstraint("id", notnull_constraint);

  // Create a primary key index and added primary key constraint to the 'name'
  // column
  oid_t primary_key_index_oid = 50002;
  CreateSamplePrimaryKeyIndex(table_name, primary_key_index_oid);

  // Create a unique index and added unique constraint to the 'time' column
  oid_t unique_index_oid = 50003;
  CreateSampleUniqueIndex(table_name, unique_index_oid);

  // Create a reference table and foreign key constraint and added unique
  // constraint to the 'salary' column
  std::string pktable_name = "pktable";
  oid_t pktable_oid = 50004;
  CreateSampleForeignKey(pktable_oid, pktable_name, columns, table_oid);

  // Check the first column's constraint
  catalog::Column column = schema->GetColumn(0);
  CheckColumnWithConstraint(column, CONSTRAINT_TYPE_NOTNULL, "", 1);

  // Check the second column's constraint and index
  column = schema->GetColumn(1);
  CheckColumnWithConstraint(column, CONSTRAINT_TYPE_PRIMARY,
                            table_name + "_pkey", 1);
  index::Index *index = table->GetIndexWithOid(primary_key_index_oid);
  CheckIndex(index, table_name + "_pkey", 1, INDEX_TYPE_BTREE,
             INDEX_CONSTRAINT_TYPE_PRIMARY_KEY, true);

  // Check the third column's constraint and index
  column = schema->GetColumn(2);
  CheckColumnWithConstraint(column, CONSTRAINT_TYPE_UNIQUE, table_name + "_key",
                            1);
  index = table->GetIndexWithOid(unique_index_oid);
  CheckIndex(index, table_name + "_key", 1, INDEX_TYPE_BTREE,
             INDEX_CONSTRAINT_TYPE_UNIQUE, true);

  // Check the fourth column's constraint and foreign key
  column = schema->GetColumn(3);
  CheckColumnWithConstraint(column, CONSTRAINT_TYPE_FOREIGN,
                            "THIS_IS_FOREIGN_CONSTRAINT", 1, 0);
  catalog::ForeignKey *pktable = table->GetForeignKey(0);
  CheckForeignKey(pktable, pktable_oid, "THIS_IS_FOREIGN_CONSTRAINT", 1, 1, 'r',
                  'c');

  // Drop the table
  status = DDLTable::DropTable(table_oid);
  assert(status);

  // Drop the table
  status = DDLTable::DropTable(pktable_oid);
  assert(status);

  LOG_INFO(":::::: %s DONE\n", __func__);
}

/**
 * @brief Test DDL and DML together. Create a table and drop it. Create a table
 *  again and insert tuple into the table.
 */
void BridgeTest::DDL_MIX_TEST_2() {
  auto &manager = catalog::Manager::GetInstance();
  storage::Database *db =
      manager.GetDatabaseWithOid(Bridge::GetCurrentDatabaseOid());

  // Get the simple columns
  std::vector<catalog::Column> columns = CreateSimpleColumns();

  // Table name and oid
  std::string table_name = "ddl_dml_mix_test_table";
  oid_t table_oid = 60001;

  // Create a table
  bool status = DDLTable::CreateTable(table_oid, table_name, columns);
  assert(status);

  // Drop the table
  status = DDLTable::DropTable(table_oid);
  assert(status);

  // Create a table again
  status = DDLTable::CreateTable(table_oid, table_name, columns);
  assert(status);

  // Get the table pointer and schema
  storage::DataTable *table = db->GetTableWithOid(table_oid);
  catalog::Schema *schema = table->GetSchema();

  // Ensure that the tile group is as expected.
  assert(schema->GetColumnCount() == 4);

  // Insert tuples
  const bool allocate = true;

  auto &txn_manager = concurrency::TransactionManager::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  for (int col_itr = 0; col_itr < 5; col_itr++) {
    storage::Tuple tuple(schema, allocate);

    // Setting values
    Value integerValue = ValueFactory::GetIntegerValue(243432);
    Value stringValue = ValueFactory::GetStringValue("dude");
    Value timestampValue = ValueFactory::GetTimestampValue(10.22);
    Value doubleValue = ValueFactory::GetDoubleValue(244643.1236);

    tuple.SetValue(0, integerValue);
    tuple.SetValue(1, stringValue);
    tuple.SetValue(2, timestampValue);
    tuple.SetValue(3, doubleValue);

    table->InsertTuple(txn, &tuple);

    assert(tuple.GetValue(0) == integerValue);
    assert(tuple.GetValue(1) == stringValue);
    assert(tuple.GetValue(2) == timestampValue);
    assert(tuple.GetValue(3) == doubleValue);
  }

  txn_manager.CommitTransaction(txn);

  // Drop the table
  status = DDLTable::DropTable(table_oid);
  assert(status);

  LOG_INFO(":::::: %s DONE\n", __func__);
}

}  // End bridge namespace
}  // End peloton namespace
