/*-------------------------------------------------------------------------
 *
 * transaction.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/common/transaction.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "catalog/transaction.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// Transaction
//===--------------------------------------------------------------------===//

void Transaction::RecordInsert(const storage::Tile* tile, id_t offset) {
  {
    std::lock_guard<std::mutex> insert_lock(txn_mutex);

    inserted_tuples[tile].push_back(offset);
  }
}

void Transaction::RecordDelete(const storage::Tile* tile, id_t offset) {
  {
    std::lock_guard<std::mutex> delete_lock(txn_mutex);

    deleted_tuples[tile].push_back(offset);
  }
}

bool Transaction::HasInsertedTuples(const storage::Tile* tile) const {
  auto tile_itr = inserted_tuples.find(tile);

  if(tile_itr != inserted_tuples.end() && !tile_itr->second.empty())
    return true;

  return false;
}

bool Transaction::HasDeletedTuples(const storage::Tile* tile) const {
  auto tile_itr = deleted_tuples.find(tile);

  if(tile_itr != deleted_tuples.end() && !tile_itr->second.empty())
    return true;

  return false;
}

const std::vector<id_t>& Transaction::GetInsertedTuples(const storage::Tile* tile) {
  return inserted_tuples[tile];
}

const std::vector<id_t>& Transaction::GetDeletedTuples(const storage::Tile* tile) {
  return deleted_tuples[tile];
}

void Transaction::IncrementRefCount() {
  ++ref_count;
}

void Transaction::DecrementRefCount() {
  assert(ref_count > 0);

  // drop txn when ref count reaches 1
  if (ref_count.fetch_sub(1) == 1) {
    delete this;
  }
}

std::ostream& operator<<(std::ostream& os, const Transaction& txn) {

  os << "\t Txn ID : " << txn.txn_id << " Commit ID : " << txn.visible_cid
      << " Next @ : " << txn.next << "\n";

  return os;
}


} // End catalog namespace
} // End nstore namespace
