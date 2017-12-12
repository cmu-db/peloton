//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// statement.h
//
// Identification: src/include/common/statement_cache.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <unordered_map>

#include "cache.h"
#include "type/types.h"

namespace peloton {
// The statement cache that caches statement.
// It would pop out statements when the cache hits limit
// It would also replan the statement7
class StatementCache {
 public:
  // Add a statement to the cache
  void AddStatement(std::shared_ptr<Statement> stmt);

  // Get the statement by its name;
  std::shared_ptr<Statement> GetStatement(std::string name);

  // Notify the Statement Cache a table id that is invalidated
  void NotifyInvalidTable(oid_t table_id);

 private:
  // Private members
  
  // StatementName -> Statement
  Cache<std::string, Statement> statement_cache_;
  
  // TableOid -> Statements
  std::unordered_map<oid_t, std::unordered_set<Statement *>> table_statement_cache_;

  // Queue to receive invalid table oids from executor
  LockFreeQueue<oid_t> invalid_table_queue_;


  // Private functions

  // Fetch the invalid table queue and mark all related plans in this cache
  // to be invalid, also clear the queue.
  void UpdateFromInvalidTableQueue();
  
};
}  // namespace peloton
