//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// statement_cache_manager.cpp
//
// Identification: src/include/common/statement_cache_manager.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/statement_cache_manager.h"

namespace peloton {

void StatementCacheManager::RegisterStatementCache(StatementCache *stmt_cache) {
  statement_caches_.Insert(stmt_cache, stmt_cache);
}

void StatementCacheManager::UnRegisterStatementCache(StatementCache *stmt_cache) {
  statement_caches_.Erase(stmt_cache);
}

void StatementCacheManager::InvalidateTableOid(oid_t table_id) {
  if (statement_caches_.IsEmpty()) 
    return;

  // Lock the table by grabbing the iterator
  LOG_TRACE("Locking the table to get the iterator");
  auto iterator = statement_caches_.GetIterator();

  // Iterate each plan cache
  for (auto &iter : iterator) {
    iter.first->NotifyInvalidTable(table_id);
  }
  // Automatically release the table;
}

void StatementCacheManager::InvalidateTableOids(std::set<oid_t> &table_ids) {
  if (table_ids.empty() || statement_caches_.IsEmpty())
    return;

  // Lock the table by grabbing the iterator
  LOG_TRACE("Locking the table to get the iterator");
  auto iterator = statement_caches_.GetIterator();

  // Iterate each plan cache and notify every table id
  for (auto &iter : iterator) {
    for (auto &table_id : table_ids)
      iter.first->NotifyInvalidTable(table_id);
  }
  // Automatically release the table;
}
}
 