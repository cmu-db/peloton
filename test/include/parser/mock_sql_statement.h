//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// mock_parse_tree.h
//
// Identification: test/include/parser/mock_parse_tree.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "gmock/gmock.h"

#include "parser/sql_statement.h"

namespace peloton {

class SqlNodeVisitor;

namespace parser {
namespace test {

class MockSQLStatement : public parser::SQLStatement {
 public:
  MockSQLStatement() : parser::SQLStatement(StatementType::SELECT) {}

  MOCK_METHOD1(Accept, void(SqlNodeVisitor *v));
};

}  // namespace test
}  // namespace parser
}  // namespace peloton
