//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// statement.h
//
// Identification: src/include/common/statement_cache_manager.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "container/cuckoo_map.h"
#include "common/statement_cache.h"

namespace peloton {

class StatementCacheManager;

static std::shared_ptr<StatementCacheManager> statement_cache_manager;

class StatementCacheManager {

 public:
  // Register a statement cache to this statement manager
  // Called by protocol handler when building up a connection
  void RegisterStatementCache(StatementCache *stmt_cache);

  // Remove the statement cache from this statement manager
  // Called by protocol handler when tearing down a connection
  void UnRegisterStatementCache(StatementCache *stmt_cache);

  // Notify the manager that the table schema or index 
  // is changed now.
  // Called in executor
  // WARNING: 
  // Be conservative to call this function as it would block
  // the whole statement caches map and prevent them from register
  // and unregister to the manager. It would therefore block new 
  // connections coming in and old connection tearing down. 
  // However, current connections can still retrieve updated plans from their plan cache.
  void InvalidateTableOid(oid_t table_id);

  // TODO (Tianyi) : remove this singloton to peloton instance
  // Initialize an statement cache manager instance
  inline static void Init(){
    statement_cache_manager = std::make_shared<StatementCacheManager>();
  }

  // Get the instance of statement cache manager
  inline static std::shared_ptr<StatementCacheManager> GetStmtCacheManager() {
    return statement_cache_manager;
  }

private:
  // A lock free hash map
  CuckooMap<StatementCache*, StatementCache*> statement_caches_;

  // Instance of statement cache manager
};

}  // namespace peloton