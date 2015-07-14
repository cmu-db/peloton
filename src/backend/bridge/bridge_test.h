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

namespace peloton {
namespace test {

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

  static void TestCatalog();

  static void DDL_CreateTable();
  static void DDL_CreateTable_TEST_INVALID_OID();
  static void DDL_CreateTable_TEST_BASIC_COLUMNS();
  static void DDL_CreateTable_TEST_NOTNULL_CONSTRAINT();

};

} // End test namespace
} // End peloton namespace
