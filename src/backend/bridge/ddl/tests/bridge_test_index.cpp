//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// bridge_test_index.cpp
//
// Identification: src/backend/bridge/ddl/tests/bridge_test_index.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "bridge_test.h"

#include "backend/bridge/ddl/bridge.h"
#include "backend/bridge/ddl/ddl.h"
#include "backend/bridge/ddl/ddl_table.h"
#include "backend/bridge/ddl/ddl_index.h"

namespace peloton {
namespace bridge {

/**
 * @brief Test "DDLIndex" function
 */
void BridgeTest::DDL_Index_TEST() {
  DDL_CreateIndex_TEST_WITH_INVALID_OID();

  DDL_CreateIndex_TEST_WITH_NO_TABLE_NAME();

  DDL_CreateIndex_TEST_WITH_TABLE();
}

/**
 * @brief CreateIndex with INVALID OID
 *        It MUST return false
 */
void BridgeTest::DDL_CreateIndex_TEST_WITH_INVALID_OID() {
  std::vector<std::string> key_column_names;
  key_column_names.push_back("id");
  key_column_names.push_back("name");

  std::string table_name = "test_table";

  IndexInfo *index_info;
  index_info = new IndexInfo(
      "test_index_with_invalid_oid", INVALID_OID, table_name, INDEX_TYPE_BTREE,
      INDEX_CONSTRAINT_TYPE_DEFAULT, true, key_column_names);

  bool status = DDLIndex::CreateIndex(*index_info);

  // CHECK :: status must be false
  assert(status == false);

  LOG_INFO(":::::: %s DONE\n", __func__);
}

/**
 * @brief CreateIndex with NO TABLE NAME
 *        It MUST return false
 */
void BridgeTest::DDL_CreateIndex_TEST_WITH_NO_TABLE_NAME() {
  std::vector<std::string> key_column_names;
  key_column_names.push_back("id");
  key_column_names.push_back("name");

  std::string table_name = "";

  IndexInfo *index_info;
  index_info = new IndexInfo("test_index_with_no_table_name", 30001, table_name,
                             INDEX_TYPE_BTREE, INDEX_CONSTRAINT_TYPE_DEFAULT,
                             true, key_column_names);

  bool status = DDLIndex::CreateIndex(*index_info);

  // CHECK :: status must be false
  assert(status == false);

  LOG_INFO(":::::: %s DONE\n", __func__);
}

/**
 * @brief Create an index with simple table
 */
void BridgeTest::DDL_CreateIndex_TEST_WITH_TABLE() {
  // Get the simple columns
  std::vector<catalog::Column> columns = CreateSimpleColumns();
  assert(columns.size() > 0);

  // Table name and oid
  std::string table_name = "simple_table";
  oid_t table_oid = 30002;

  // Create a table
  bool status = DDLTable::CreateTable(table_oid, table_name, columns);
  assert(status);

  // Create an index info
  std::vector<std::string> key_column_names;
  key_column_names.push_back("id");
  key_column_names.push_back("name");

  IndexInfo *index_info;
  index_info =
      new IndexInfo("simple_index", 30003, table_name, INDEX_TYPE_BTREE,
                    INDEX_CONSTRAINT_TYPE_DEFAULT, true, key_column_names);

  // Create an index
  status = DDLIndex::CreateIndex(*index_info);

  // CHECK :: status must be false
  assert(status);

  // Drop the table
  status = DDLTable::DropTable(table_oid);
  assert(status);

  LOG_INFO(":::::: %s DONE\n", __func__);
}

}  // End bridge namespace
}  // End peloton namespace
