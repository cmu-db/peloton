/*-------------------------------------------------------------------------
 *
 * btree_multimap_index.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/index/btree_multimap_index.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "index/btree_multimap_index.h"

namespace nstore {
namespace index {


BtreeMultimapIndex::BtreeMultimapIndex(const IndexMetadata &metadata)
: Index(metadata) {

  // Start manager
  size_t pool_size = 2 * 1024 * 1024;
  int bits = 16;

  btree_manager = bt_mgr ((char *) metadata.identifier.c_str(), bits, pool_size);

  btree_db = bt_open (btree_manager);
}

BtreeMultimapIndex::~BtreeMultimapIndex(){

  // Close manager
  bt_mgrclose(btree_manager);

}

bool BtreeMultimapIndex::InsertEntry(storage::Tuple *key, ItemPointer location) {

  BTERR status = bt_insertkey (btree_db, (unsigned char *) key->GetData(), key->GetLength(), 0, &location, sizeof(ItemPointer), 0);

  if(status == BTERR_ok)
    return true;

  return false;
}

bool BtreeMultimapIndex::DeleteEntry(storage::Tuple *key){

  BTERR status = bt_deletekey (btree_db, (unsigned char *) key->GetData(), key->GetLength(), 0);

  if(status == BTERR_ok)
    return true;

  return false;
}

bool BtreeMultimapIndex::Exists(storage::Tuple *key) const{

  int found = bt_findkey (btree_db, (unsigned char *) key->GetData(), key->GetLength(), nullptr, 0);

  if(found != -1)
    return true;

  return false;
}

std::vector<ItemPointer> BtreeMultimapIndex::GetLocationsForKey(storage::Tuple *key) const{
  std::vector<ItemPointer> result;

  return result;
}

std::vector<ItemPointer> BtreeMultimapIndex::GetLocationsForKeyBetween(storage::Tuple *start, storage::Tuple *end) const{
  std::vector<ItemPointer> result;

  return result;
}

std::vector<ItemPointer> BtreeMultimapIndex::GetLocationsForKeyLT(storage::Tuple *key) const{
  std::vector<ItemPointer> result;

  return result;
}

std::vector<ItemPointer> BtreeMultimapIndex::GetLocationsForKeyLTE(storage::Tuple *key) const{
  std::vector<ItemPointer> result;

  return result;
}

std::vector<ItemPointer> BtreeMultimapIndex::GetLocationsForKeyGT(storage::Tuple *key) const{
  std::vector<ItemPointer> result;

  return result;
}

std::vector<ItemPointer> BtreeMultimapIndex::GetLocationsForKeyGTE(storage::Tuple *key) const{
  std::vector<ItemPointer> result;

  return result;
}

} // End index namespace
} // End nstore namespace



