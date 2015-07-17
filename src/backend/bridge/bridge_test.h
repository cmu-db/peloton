/*-------------------------------------------------------------------------
 *
 * bridge_test.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/tests/bridge/bridge_test.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <vector>

namespace peloton {

namespace catalog {
class Column;
}

namespace bridge {

//===--------------------------------------------------------------------===//
// Bridge Tests
//===--------------------------------------------------------------------===//

class BridgeTest {

 public:
  BridgeTest(const BridgeTest &) = delete;
  BridgeTest& operator=(const BridgeTest &) = delete;
  BridgeTest(BridgeTest &&) = delete;
  BridgeTest& operator=(BridgeTest &&) = delete;

  static void RunTests();

 private:

  //===--------------------------------------------------------------------===//
  // Tests
  //===--------------------------------------------------------------------===//

  static void DDL_CreateTable_TEST();

  static void DDL_CreateTable_TEST_INVALID_OID();

  static void DDL_CreateTable_TEST_COLUMNS();

  static void DDL_CreateTable_TEST_COLUMN_CONSTRAINTS();

  //===--------------------------------------------------------------------===//
  // Utilities
  //===--------------------------------------------------------------------===//

  static std::vector<catalog::Column> CreateSimpleColumns();

};

} // End bridge namespace
} // End peloton namespace
