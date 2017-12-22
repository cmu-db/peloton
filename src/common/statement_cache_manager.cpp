//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// statement.h
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
  // Lock the table by grabing the iterator
  auto iterator = statement_caches_.GetIterator();

  // Iterate each plan cache
  for (auto &iter : iterator) {
    iter.first->NotifyInvalidTable(table_id);
  }
  // Automatically release the table;
}
}
 