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

#include "common/types.h"
#include "storage/tuple.h"

#include "index/index.h"
#include "index/concurrent_btree.h"

#include <vector>
#include <string>
#include "catalog/manager.h"

namespace nstore {
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

  bool UpdateEntry(const storage::Tuple *key, ItemPointer location, ItemPointer old_location);

  bool DeleteEntry(const storage::Tuple *key);

  bool Exists(const storage::Tuple *key) const;

  std::vector<ItemPointer> Scan() const;

  std::vector<ItemPointer> GetLocationsForKey(storage::Tuple *key) const;

  std::vector<ItemPointer> GetLocationsForKeyBetween(storage::Tuple *start, storage::Tuple *end) const;

  std::vector<ItemPointer> GetLocationsForKeyLT(storage::Tuple *key) const;

  std::vector<ItemPointer> GetLocationsForKeyLTE(storage::Tuple *key) const;

  std::vector<ItemPointer> GetLocationsForKeyGT(storage::Tuple *key) const;

  std::vector<ItemPointer> GetLocationsForKeyGTE(storage::Tuple *key)  const;

  std::string GetTypeName() const {
    return "BtreeMultimap";
  }

 protected:

  // Btree manager
  BtMgr *btree_manager;

  BtDb *btree_db;

  // unique keys
  int unique_keys;
};

} // End index namespace
} // End nstore namespace

