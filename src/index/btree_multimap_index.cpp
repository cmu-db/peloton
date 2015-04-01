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
}

BtreeMultimapIndex::~BtreeMultimapIndex(){
}

bool BtreeMultimapIndex::InsertEntry(storage::Tuple *key, ItemPointer location){
  return false;
}

bool BtreeMultimapIndex::DeleteEntry(storage::Tuple *key){
  return false;
}

bool BtreeMultimapIndex::UpdateEntry(storage::Tuple *key, ItemPointer location){
  return false;
}

bool BtreeMultimapIndex::Exists(storage::Tuple *key) const{
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



