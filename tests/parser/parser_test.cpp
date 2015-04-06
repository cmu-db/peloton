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

  for(auto query : queries) {
    parser::Parser::ParseSQLString(query);
  }

}

} // End test namespace
} // End nstore namespace

