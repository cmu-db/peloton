/*-------------------------------------------------------------------------
 *
 * bridge_test_database.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/tests/bridge/bridge_test_database.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "bridge_test.h"

#include "backend/bridge/ddl/bridge.h"
#include "backend/bridge/ddl/ddl.h"
#include "backend/bridge/ddl/ddl_database.h"

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
  assert(status == false);

  std::cout << ":::::: " << __func__ << " DONE\n";
}

/**
 * @brief CreateDatabase with VALID OID
 *        It MUST return true
 */
void BridgeTest::DDL_CreateDatabase_TEST_WITH_VALID_OID() {
  bool status = DDLDatabase::CreateDatabase(12345);

  // CHECK :: status must be true
  assert(status);

  std::cout << ":::::: " << __func__ << " DONE\n";
}

}  // End bridge namespace
}  // End peloton namespace
