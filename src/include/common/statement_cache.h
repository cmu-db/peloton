//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// statement_cache.h
//
// Identification: src/include/common/statement_cache.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <unordered_map>

#include "common/internal_types.h"
#include "container/lock_free_queue.h"
#include "statement.h"

namespace peloton {

#define DEFAULT_STATEMENT_CACHE_INVALID_QUEUE_SIZE 5

// The statement cache that caches statement.
// It would also mark statement if it need to replan
class StatementCache {
  typedef std::shared_ptr<Statement> StatementPtr;
  typedef std::unordered_map<std::string, StatementPtr> NameMap;
  typedef std::unordered_map<oid_t, std::unordered_set<StatementPtr>> TableRef;

  // Private members

  // StatementName -> Statement
  // Note that we use unordered map instead of (LRU) Cache because
  // (LRU) cache does not expose iterface that retrieve the popped-out
  // entries. This would make table_ref_ not up-to-date.
  NameMap statement_map_;

  // TableOid -> Statements
  TableRef table_ref_;

  // Queue to receive invalid table oids from executor
  LockFreeQueue<oid_t> invalid_table_queue_;

 public:
  StatementCache()
      : invalid_table_queue_(DEFAULT_STATEMENT_CACHE_INVALID_QUEUE_SIZE) {}

  // Add a statement to the cache
  void AddStatement(std::shared_ptr<Statement> stmt);

  // Get the statement by its name;
  std::shared_ptr<Statement> GetStatement(std::string name);

  // Delete the statement
  void DeleteStatement(std::string name);

  // Notify the Statement Cache a table id that is invalidated
  void NotifyInvalidTable(oid_t table_id);

  // Clear the cache
  void Clear();

 private:
  void UpdateFromInvalidTableQueue();
};
}  // namespace peloton
