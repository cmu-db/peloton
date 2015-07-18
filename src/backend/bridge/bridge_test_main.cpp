/*-------------------------------------------------------------------------
 *
 * bridge_test_main.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/tests/bridge/bridge_test_main.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "bridge_test.h"

#include "backend/bridge/bridge.h"
#include "backend/bridge/ddl_database.h"

namespace peloton {
namespace bridge {

void BridgeTest::RunTests() {
  // Create the new storage database and add it to the manager
  DDLDatabase::CreateDatabase( Bridge::GetCurrentDatabaseOid() );

  std::cout<< ":::::::::::::  TEST CASES START :::::::::::::\n";

  DDL_Database_TEST();

  DDL_Table_TEST();

  DDL_Index_TEST();

  DDL_MIX_TEST();

  std::cout<< ":::::::::::::  TEST CASES END   :::::::::::::\n";
}

} // End bridge namespace
} // End peloton namespace

