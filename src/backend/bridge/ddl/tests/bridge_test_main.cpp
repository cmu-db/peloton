//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// bridge_test_main.cpp
//
// Identification: src/backend/bridge/ddl/tests/bridge_test_main.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "bridge_test.h"

#include "backend/bridge/ddl/bridge.h"
#include "backend/bridge/ddl/ddl_database.h"

namespace peloton {
namespace bridge {

void BridgeTest::RunTests() {
  // Create the new storage database and add it to the manager
  DDLDatabase::CreateDatabase(Bridge::GetCurrentDatabaseOid());

  LOG_INFO(":::::::::::::  TEST CASES START :::::::::::::\n");

  DDL_Database_TEST();

  DDL_Table_TEST();

  DDL_Index_TEST();

  DDL_MIX_TEST();

  LOG_INFO(":::::::::::::  TEST CASES END   :::::::::::::\n");
}

}  // End bridge namespace
}  // End peloton namespace
