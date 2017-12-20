//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// statement_cache_test.cpp
//
// Identification: test/common/statement_cache_manager_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/statement_cache_manager.h"
#include "common/statement_cache.h"
#include "common/statement.h"

#include "common/harness.h"


namespace peloton {
namespace test {
// Tests for both statement cache
class StatementCacheTests : public PelotonTest {};
  
  TEST_F(StatementCacheTests, CorrectnessTest) {
    // Register the statement cache to the statement cache manager
    auto statement_cache_manager = std::make_shared<StatementCacheManager>();
    auto statement_cache = std::make_shared<StatementCache>();
    statement_cache_manager->RegisterStatementCache(statement_cache);
    std::set<oid_t> ref_table = {0};

    std::string string_name = "foo";
    std::string query = "bar";
    auto statement = std::make_shared<Statement>(string_name, query);
    statement->SetReferencedTables(ref_table);

    EXPECT_TRUE(!statement->GetNeedsPlan());
    statement_cache->AddStatement(statement);

    // Invalidate table oid 0
    statement_cache_manager->InvalidateTableOid(0);

    // The plan need to replan now
    statement = statement_cache->GetStatement(string_name);
    EXPECT_TRUE(statement->GetNeedsPlan());

    // Unregister the statement cache and invalidate again
    statement->SetNeedsPlan(false);
    statement_cache_manager->UnRegisterStatementCache(statement_cache);
    statement_cache_manager->InvalidateTableOid(0);
    statement = statement_cache->GetStatement(string_name);
    // This one should not be affected, since the cache is already un-registered
    EXPECT_TRUE(!statement->GetNeedsPlan());
  }

}
}
