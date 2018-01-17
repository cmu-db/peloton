//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// statement_cache.cpp
//
// Identification: src/include/common/statement_cache.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <unordered_set>

#include "common/statement_cache.h"

namespace peloton {
// Add a statement to the cache
void StatementCache::AddStatement(std::shared_ptr<Statement> stmt) {
  UpdateFromInvalidTableQueue();
  statement_map_[stmt->GetStatementName()] = stmt;
  for (auto table_id : stmt->GetReferencedTables()) {
    table_ref_[table_id].insert(stmt);
  }
}

// Get the statement by its name;
std::shared_ptr<Statement> StatementCache::GetStatement(std::string name) {
  UpdateFromInvalidTableQueue();
  return statement_map_[name];
}

  // Delete the statement
void StatementCache::DeleteStatement(std::string name) {
  if (!statement_map_.count(name)) {
    return;
  }
  auto to_delete = statement_map_[name];
  for (auto table_id : to_delete->GetReferencedTables()) {
    table_ref_[table_id].erase(to_delete);
  }
  statement_map_.erase(name);
}

// Notify the Statement Cache a table id that is invalidated
void StatementCache::NotifyInvalidTable(oid_t table_id) {
  invalid_table_queue_.Enqueue(table_id);
}

void StatementCache::UpdateFromInvalidTableQueue() {
  std::unordered_set<oid_t> invalid_set;
  oid_t invalid_oid;
  while (invalid_table_queue_.Dequeue(invalid_oid)) {
    if (table_ref_.count(invalid_oid)) invalid_set.insert(invalid_oid);
  }

  if (invalid_set.size()) {
    for (oid_t id : invalid_set)
      for (auto &statement : table_ref_[id]) statement->SetNeedsReplan(true);
  }
}

void StatementCache::Clear() {
  statement_map_.clear();
  table_ref_.clear();
  while (!invalid_table_queue_.IsEmpty()) {
    oid_t dummy;
    invalid_table_queue_.Dequeue(dummy);
  }
}

}  // namespace peloton
