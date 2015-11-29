//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// bridge_test_database.cpp
//
// Identification: src/backend/bridge/ddl/tests/bridge_test_database.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "bridge_test.h"

#include "backend/bridge/ddl/bridge.h"
#include "backend/bridge/ddl/ddl.h"
#include "backend/bridge/ddl/ddl_database.h"
#include "backend/common/exception.h"

namespace peloton {
namespace bridge {

/**
 * @brief Test "DDLDatabase" function
 */
void BridgeTest::DDL_Database_TEST() {
  DDL_CreateDatabase_TEST_WITH_INVALID_OID();

  DDL_CreateDatabase_TEST_WITH_VALID_OID();
}

/**
 * @brief CreateDatabase with INVALID OID
 *        It MUST return false
 */
void BridgeTest::DDL_CreateDatabase_TEST_WITH_INVALID_OID() {
  bool status = DDLDatabase::CreateDatabase(INVALID_OID);
  // CHECK :: status must be false
  if(status != false)
    throw CatalogException("Could create database");

  LOG_INFO(":::::: %s DONE\n", __func__);
}

/**
 * @brief CreateDatabase with VALID OID
 *        It MUST return true
 */
void BridgeTest::DDL_CreateDatabase_TEST_WITH_VALID_OID() {
  bool status = DDLDatabase::CreateDatabase(12345);

  // CHECK :: status must be true
  if(status == false)
    throw CatalogException("Could not create database");

  status = DDLDatabase::DropDatabase(12345);
  assert(status);

  LOG_INFO(":::::: %s DONE\n", __func__);
}

}  // End bridge namespace
}  // End peloton namespace
