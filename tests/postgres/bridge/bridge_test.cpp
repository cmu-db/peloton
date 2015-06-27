/*-------------------------------------------------------------------------
 *
 * parser_test.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/tests/parser/parser_test.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "gtest/gtest.h"
#include "harness.h"

#include <iostream>
#include <string>
#include <cstdlib>

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Bridge Tests
//===--------------------------------------------------------------------===//

TEST(BridgeTests, BasicTest) {
  
  int res;
  std::string db_name ("bridge_test_db");
  std::string db_filesystem_path = "/tmp/" + db_name;
  std::string cmd;

  cmd = "rm -rf " + db_filesystem_path;
  res = system(cmd.c_str());
  assert(res == 0);

  cmd = "initdb " + db_filesystem_path;
  res = system(cmd.c_str());
  assert(res == 0);

  cmd = "pg_ctl -D " + db_filesystem_path + " start -o -testmode=1"; // 1 : bridge test mode 
  res = system(cmd.c_str());
  assert(res == 0);

  // TODO: Make it time insensitive 
  // Wait for the server before running the tests.
  sleep(3);

  std::cout << "\n\n";
  std::cout << "++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n";

  res = system("psql postgres <<EOF");
  assert(res == 0);

  std::cout << "++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n";
  std::cout << "\n\n";
  
  cmd = "pg_ctl -D " + db_filesystem_path + " stop ";
  res = system(cmd.c_str());
  assert(res == 0);

}

} // End test namespace
} // End peloton namespace

