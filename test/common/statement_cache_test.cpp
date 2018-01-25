//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// statement_cache_test.cpp
//
// Identification: test/common/statement_cache_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/statement_cache.h"
#include "common/statement.h"

#include "common/harness.h"

namespace peloton {
namespace test {
// Tests for both statement cache
class StatementCacheTests : public PelotonTest {};

// Test statementCache add and get statement
TEST_F(StatementCacheTests, AddGetTest) {
  std::string string_name = "foo";
  std::string query = "SELECT * FROM TEST";
  auto statement = std::make_shared<Statement>(string_name, query);
  EXPECT_EQ(string_name, statement->GetStatementName());
  EXPECT_EQ(query, statement->GetQueryString());
  EXPECT_TRUE(!statement->GetNeedsReplan());

  auto statement_cache = std::make_shared<StatementCache>();
  statement_cache->AddStatement(statement);
  auto got_statement = statement_cache->GetStatement(string_name);
  EXPECT_EQ(statement, got_statement);
}

// Test add and get unnamed statement
TEST_F(StatementCacheTests, UnnamedStatementTest) {
  // An empty string for name of unnamed statement
  std::string unname;
  std::string query = "SELECT * FROM TEST";
  auto statement = std::make_shared<Statement>(unname, query);

  auto statement_cache = std::make_shared<StatementCache>();
  statement_cache->AddStatement(statement);
  auto got_statement = statement_cache->GetStatement(unname);
  EXPECT_EQ(statement, got_statement);
}

// Test the NotifyInvalidTable() method works
TEST_F(StatementCacheTests, DisableTableTest) {
  // Statement vector
  std::shared_ptr<StatementCache> cache = std::make_shared<StatementCache>();

  std::vector<std::shared_ptr<Statement>> statements;
  std::set<oid_t> ref_table = {0, 1, 2, 3};
  std::string query = "SELECT * FROM TEST";

  LOG_INFO("Pareparing statements");
  // Prepare the 4 statements
  for (size_t i = 0; i < 4; i++) {
    std::string name = std::to_string(i);
    ref_table.erase(i);
    auto stmt = std::make_shared<Statement>(name, query);
    stmt->SetReferencedTables(ref_table);
    statements.push_back(stmt);
    ref_table.insert(i);
  }

  oid_t table = 0;
  LOG_INFO("Putting statements into cache");
  // Insert statements in to the cache
  for (auto &stmt : statements) {
    EXPECT_EQ(3, stmt->GetReferencedTables().size());
    EXPECT_EQ(0, stmt->GetReferencedTables().count(table));
    for (oid_t t_i = 0; t_i < 4; t_i++) {
      // Reference table should contain all of 0 - 3 except itself
      EXPECT_EQ(t_i != table, stmt->GetReferencedTables().count(t_i));
    }
    table++;
    EXPECT_TRUE(!stmt->GetNeedsReplan());

    cache->AddStatement(stmt);
  }

  LOG_INFO("Notify cache the disabled table oid 2");
  // Disable the cache
  cache->NotifyInvalidTable(2);

  // plan 0 1 3 need to replan while 2 do not
  for (oid_t i = 0; i < 4; i++) {
    std::string name = std::to_string(i);
    auto stmt = cache->GetStatement(name);
    EXPECT_NE(nullptr, stmt);
    if (i != 2) {
      EXPECT_TRUE(stmt->GetNeedsReplan());
    } else {
      EXPECT_FALSE(stmt->GetNeedsReplan());
    }
  }
}


}  // namespace test
}  // namespace peloton
