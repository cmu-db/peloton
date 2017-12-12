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
#include "statement_cache.h"

namespace peloton {
class StatementCacheManager {
 public:
  // Register a statement cache to this statement manager
  // Called by protocol handler when building up a connection
  void RegisterStatementCache(StatementCache &stmt_cache);

  // Remove the statement cache from this statement manager
  // Called by protocol handler when tearing down a connection
  void UnRegisterStatementCache(StatementCache &stmt_cache);

  // Notify the manager that the table schema or index 
  // is changed now.
  // Called in executor
  // WARNING: 
  // Be conservative to call this function as it would block
  // the whole statement caches map and prevent them from register
  // and unregister to the manager. It eould therefore block new 
  // connections coming in and old connection tearing down. 
  // However, current connection can still retrieve updated plans from their plan cache.
  void InvalidateTableOid(oid_t table_id);

private:
  // A lock free hash map
  CuckooMap<StatementCache, nullptr> statement_caches;
}
}  // namespace peloton