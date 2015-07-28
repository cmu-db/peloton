/*-------------------------------------------------------------------------
 *
 * btree_multimap_index.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/common/types.h"
#include "backend/storage/tuple.h"

#include "backend/index/index.h"
#include "backend/index/concurrent_btree.h"

#include <vector>
#include <string>
#include "backend/catalog/manager.h"

#include <atomic>

namespace peloton {
namespace index {

/**
 * B+tree-based index implementation.
 *
 * @see Index
 */
class BtreeMultimapIndex : public Index {
  friend class IndexFactory;

 public:
  BtreeMultimapIndex(IndexMetadata *metadata);

  ~BtreeMultimapIndex();

  bool InsertEntry(const storage::Tuple *key, ItemPointer location);

  bool DeleteEntry(const storage::Tuple *key);

  bool Exists(const storage::Tuple *key) const;

  std::vector<ItemPointer> Scan() const;

  std::vector<ItemPointer> GetLocationsForKey(const storage::Tuple *key) const;

  std::vector<ItemPointer> GetLocationsForKeyBetween(
      const storage::Tuple *start, const storage::Tuple *end) const;

  std::vector<ItemPointer> GetLocationsForKeyLT(
      const storage::Tuple *key) const;

  std::vector<ItemPointer> GetLocationsForKeyLTE(
      const storage::Tuple *key) const;

  std::vector<ItemPointer> GetLocationsForKeyGT(
      const storage::Tuple *key) const;

  std::vector<ItemPointer> GetLocationsForKeyGTE(
      const storage::Tuple *key) const;

  std::string GetTypeName() const { return "BtreeMultimap"; }

 protected:
  // Btree manager
  BtMgr *btree_manager;

  BtDb *btree_db;

  // unique keys
  int unique_keys;

  std::atomic<size_t> num_keys;
};

}  // End index namespace
}  // End peloton namespace
