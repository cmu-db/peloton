/*-------------------------------------------------------------------------
 *
 * plan_transformer_test.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * Author: Ming Fang
 *
 *-------------------------------------------------------------------------
 */

#include "bridge_test.h"

#include "backend/bridge/bridge.h"

namespace peloton {
namespace test {

void BridgeTest::RunTests() {
  TestCatalog();
}

void BridgeTest::TestCatalog() {
  GetDatabaseList();
}

} // End test namespace
} // End peloton namespace

