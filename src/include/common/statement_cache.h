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

#include "statement.h"
#include "container/lock_free_queue.h"
#include "type/types.h"

namespace peloton {
// The statement cache that caches statement.
// It would also mark statement if it need to replan
class StatementCache {
  typedef std::shared_ptr<Statement> StatementPtr;
  typedef std::unordered_map<std::string, StatementPtr> \
          NameMap;
  typedef std::unordered_map<oid_t, std::vector<StatementPtr>> \
          TableRef;
  // Private members
  
  // StatementName -> Statement
  // Note that we use unordered map instead of (LRU) Cache because
  // (LRU) cache does not expose iterface that remove the popped out
  // entries. This would make table_statement_cache_ not up-to-date.
  NameMap statement_map_;
  
  // TableOid -> Statements
  TableRef table_ref_;

  // Queue to receive invalid table oids from executor
  LockFreeQueue<oid_t> invalid_table_queue_;

 public:
  // Add a statement to the cache
  void AddStatement(std::shared_ptr<Statement> stmt);

  // Get the statement by its name;
  std::shared_ptr<Statement> GetStatement(std::string name);

  // Notify the Statement Cache a table id that is invalidated
  void NotifyInvalidTable(oid_t table_id);
  
private:
  void UpdateFromInvalidTableQueue();

};
}  // namespace peloton
