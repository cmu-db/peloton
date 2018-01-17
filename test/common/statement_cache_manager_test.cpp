//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// statement_cache_manager_test.cpp
//
// Identification: test/common/statement_cache_manager_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/statement_cache_manager.h"
#include "common/statement.h"
#include "common/statement_cache.h"

#include "common/harness.h"

namespace peloton {
namespace test {
// Tests for both statement cache
class StatementCacheTests : public PelotonTest {};

TEST_F(StatementCacheTests, InvalidateOneTest) {
  // Register the statement cache to the statement cache manager
  StatementCacheManager::Init();
  auto statement_cache_manager = StatementCacheManager::GetStmtCacheManager();
  StatementCache statement_cache;
  statement_cache_manager->RegisterStatementCache(&statement_cache);
  std::set<oid_t> ref_table = {0};

  std::string string_name = "foo";
  std::string query = "SELECT * FROM TEST";
  auto statement = std::make_shared<Statement>(string_name, query);
  statement->SetReferencedTables(ref_table);

  EXPECT_TRUE(!statement->GetNeedsReplan());
  statement_cache.AddStatement(statement);

  // Invalidate table oid 0
  statement_cache_manager->InvalidateTableOid(0);

  // The plan need to replan now
  statement = statement_cache.GetStatement(string_name);
  EXPECT_TRUE(statement->GetNeedsReplan());

  // Unregister the statement cache and invalidate again
  statement->SetNeedsReplan(false);
  statement_cache_manager->UnRegisterStatementCache(&statement_cache);
  statement_cache_manager->InvalidateTableOids(ref_table);
  statement = statement_cache.GetStatement(string_name);
  // This one should not be affected, since the cache is already un-registered
  EXPECT_TRUE(!statement->GetNeedsReplan());
}

TEST_F(StatementCacheTests, InvalidateManyTest) {
  // Register the statement cache to the statement cache manager
  StatementCacheManager::Init();
  auto statement_cache_manager = StatementCacheManager::GetStmtCacheManager();
  StatementCache statement_cache;
  statement_cache_manager->RegisterStatementCache(&statement_cache);
  std::set<oid_t> ref_table = {0, 1};

  for (auto oid : ref_table) {
    std::string string_name = "foo" + oid;
    std::string query = "SELECT * FROM TEST";
    auto statement = std::make_shared<Statement>(string_name, query);
    std::set<oid_t> cur_ref_table;
    cur_ref_table.insert(oid);
    statement->SetReferencedTables(cur_ref_table);

    EXPECT_TRUE(!statement->GetNeedsReplan());
    statement_cache.AddStatement(statement);
  }

  // Invalidate table oid 0, 1
  statement_cache_manager->InvalidateTableOids(ref_table);

  // Both plans need to replan now
  for (auto oid : ref_table) {
    std::string string_name = "foo" + oid;
    auto statement = statement_cache.GetStatement(string_name);

    EXPECT_TRUE(statement->GetNeedsReplan());
  }
}
}  // namespace test
}  // namespace peloton
