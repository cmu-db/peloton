//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bridge_test_main.cpp
//
// Identification: src/backend/bridge/ddl/tests/bridge_test_main.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
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

  LOG_TRACE(":::::::::::::  TEST CASES START :::::::::::::");

  DDL_Database_TEST();

  DDL_Table_TEST();

  DDL_Index_TEST();

  DDL_MIX_TEST();

  LOG_TRACE(":::::::::::::  TEST CASES END   :::::::::::::");
}

}  // End bridge namespace
}  // End peloton namespace
