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

};

} // End test namespace
} // End peloton namespace
