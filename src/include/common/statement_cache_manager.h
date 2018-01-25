//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// statement_cache_manager.h
//
// Identification: src/include/common/statement_cache_manager.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/statement_cache.h"
#include "container/cuckoo_map.h"

namespace peloton {

// TODO(Tianyi) remove this singleton
class StatementCacheManager;
// Singleton statement_cache_manager
static std::shared_ptr<StatementCacheManager> statement_cache_manager;

/**
 * The manager that stores all the registered statement caches.
 * Those registered statement caches would be notify when some
 * table is no longer valid.
 */
class StatementCacheManager {
 public:
  /**
   * @brief Register a statement cache to this statement manager
   *  Called by protocol handler when building up a connection
   * @param stmt_cache the statement cache to be registered
   */
  void RegisterStatementCache(StatementCache *stmt_cache);

  /**
   * @brief Remove the statement cache from this statement manager
   *  Called by protocol handler when tearing down a connection
   * @param stmt_cache the statement cache to be unregister
   */
  void UnRegisterStatementCache(StatementCache *stmt_cache);


  /* WARNING:
   * Be conservative to call the following functions:
   * InvalidateTableOid()  InvalidateTableOids()
   * as they would block the whole statement caches map and prevent them 
   * from register and unregister to the manager. It would therefore block new
   * connections coming in and old connection tearing down.
   * However, current connections can still retrieve updated plans from their
   * plan cache.
   */

  /**
   * @brief Notify the manager that the statements with table id is no longer
   * valid now
   * 
   * @param table_id The table that is no longer valid
   */
  void InvalidateTableOid(oid_t table_id);

  /**
   * @brief Notify the manager that the statements with table ids is no longer
   * valid now
   * 
   * @param table_ids The tables that are no longer valid
   */
  void InvalidateTableOids(std::set<oid_t> &table_ids);

  // TODO (Tianyi) : remove this singleton to peloton instance
  /**
   *  Initialize an statement cache manager instance
   */
  inline static void Init() {
    statement_cache_manager = std::make_shared<StatementCacheManager>();
  }

  // TODO (Tianyi) : move this singleton to peloton instance
  /**
   * Get the instance of statement cache manager
   * @return the statement cache manager instance
   */
  inline static std::shared_ptr<StatementCacheManager> GetStmtCacheManager() {
    return statement_cache_manager;
  }

 private:
  /**
   * The registered statement caches
   */
  CuckooMap<StatementCache *, StatementCache *> statement_caches_;
};

}  // namespace peloton