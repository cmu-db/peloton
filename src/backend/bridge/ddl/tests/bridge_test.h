//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// bridge_test.h
//
// Identification: src/backend/bridge/ddl/tests/bridge_test.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <string>
#include <iostream>

#include <backend/common/types.h>
#include <backend/common/logger.h>

namespace peloton {

namespace catalog {
class Column;
class ForeignKey;
}

namespace index {
class Index;
}

namespace bridge {

//===--------------------------------------------------------------------===//
// Bridge Tests
//===--------------------------------------------------------------------===//

class BridgeTest {
 public:
  BridgeTest(const BridgeTest &) = delete;
  BridgeTest &operator=(const BridgeTest &) = delete;
  BridgeTest(BridgeTest &&) = delete;
  BridgeTest &operator=(BridgeTest &&) = delete;

  // Bridge Test Main Function
  static void RunTests();

 private:
  //===--------------------------------------------------------------------===//
  // Database Test
  //===--------------------------------------------------------------------===//

  static void DDL_Database_TEST();

  static void DDL_CreateDatabase_TEST_WITH_INVALID_OID();

  static void DDL_CreateDatabase_TEST_WITH_VALID_OID();

  //===--------------------------------------------------------------------===//
  // Table Test
  //===--------------------------------------------------------------------===//

  static void DDL_Table_TEST();

  static void DDL_CreateTable_TEST_WITH_INVALID_OID();

  static void DDL_CreateTable_TEST_WITH_COLUMNS();

  //===--------------------------------------------------------------------===//
  // Index Test
  //===--------------------------------------------------------------------===//

  static void DDL_Index_TEST();

  static void DDL_CreateIndex_TEST_WITH_INVALID_OID();

  static void DDL_CreateIndex_TEST_WITH_NO_TABLE_NAME();

  static void DDL_CreateIndex_TEST_WITH_TABLE();

  //===--------------------------------------------------------------------===//
  // Mix Test
  //===--------------------------------------------------------------------===//

  static void DDL_MIX_TEST();

  static void DDL_MIX_TEST_1();

  static void DDL_MIX_TEST_2();

  //===--------------------------------------------------------------------===//
  // Helper Functions
  //===--------------------------------------------------------------------===//

  static std::vector<catalog::Column> CreateSimpleColumns();

  static bool CheckColumn(catalog::Column &column, std::string column_name,
                          int length, ValueType type);

  static bool CheckColumnWithConstraint(catalog::Column &column,
                                        ConstraintType constraint_type,
                                        std::string constraint_name,
                                        unsigned int constraint_count,
                                        int foreign_key_offset = -1);

  static bool CheckIndex(index::Index *index, std::string index_name,
                         oid_t column_count, IndexType method_type,
                         IndexConstraintType constraint_type, bool unique);

  static bool CheckForeignKey(catalog::ForeignKey *foreign_key,
                              oid_t pktable_oid, std::string constraint_name,
                              unsigned int pk_column_names_count,
							  unsigned int fk_column_names_count, char fk_update_action,
                              char fk_delete_action);

  static void CreateSamplePrimaryKeyIndex(std::string table_name,
                                          oid_t index_oid);

  static void CreateSampleUniqueIndex(std::string table_name, oid_t index_oid);

  static void CreateSampleForeignKey(oid_t pktable_oid,
                                     std::string pktable_name,
                                     std::vector<catalog::Column> &columns,
                                     oid_t table_oid);

  static oid_t CreateTableInPostgres(std::string table_name);

  static bool DropTableInPostgres(std::string table_name);
};

}  // End bridge namespace
}  // End peloton namespace
