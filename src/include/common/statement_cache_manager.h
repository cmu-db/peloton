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
class StatementCacheManager {
  typedef std::shared_ptr<StatementCache> StmtCachePtr;
 public:
  // Register a statement cache to this statement manager
  // Called by protocol handler when building up a connection
  void RegisterStatementCache(StmtCachePtr stmt_cache);

  // Remove the statement cache from this statement manager
  // Called by protocol handler when tearing down a connection
  void UnRegisterStatementCache(StmtCachePtr stmt_cache);

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

private:
  // A lock free hash map
  CuckooMap<StmtCachePtr, StmtCachePtr> statement_caches_;
};
}  // namespace peloton