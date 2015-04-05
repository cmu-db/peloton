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

#include "catalog/manager.h"
#include "gtest/gtest.h"

#include "harness.h"
#include "parser/parser.h"

#include <vector>

namespace nstore {
namespace test {

//===--------------------------------------------------------------------===//
// Parser Tests
//===--------------------------------------------------------------------===//

TEST(ParserTests, BasicTest) {

  std::vector<std::string> queries;

  queries.push_back("select * from tab");

  queries.push_back("SELECT -- TOP 100"
      "objID, ra ,dec -- Get the unique object ID and coordinates"
      "FROM"
      "PhotoPrimary -- From the table containing photometric data for unique objects"
      "WHERE"
      "ra > 185 and ra < 185.1"
      "AND dec > 15 and dec < 15.1 -- that matches our criteria");

  for(auto query : queries) {
    parser::Parser::ParseSQLString(query);
  }

}

} // End test namespace
} // End nstore namespace

