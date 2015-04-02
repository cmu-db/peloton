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
  size_t pool_size = 1024;
  unsigned int bits = 16;

  btree_manager = bt_mgr ((char *) metadata.identifier.c_str(), bits, pool_size);

  btree_db = bt_open (btree_manager);

  btree_db->key_schema = key_schema;
}

BtreeMultimapIndex::~BtreeMultimapIndex(){

  // Close manager
  bt_mgrclose(btree_manager);

}

bool BtreeMultimapIndex::InsertEntry(storage::Tuple *key, ItemPointer location) {

  BTERR status = bt_insertkey (btree_db, key->GetData(), key->GetLength(), 0, &location, sizeof(ItemPointer), 0);

  printf("status :: %d \n", status);

  if(status == BTERR_ok)
    return true;

  return false;
}

bool BtreeMultimapIndex::DeleteEntry(storage::Tuple *key){

  printf("+++++++++++++++++++++++++++ Delete key :: \n");

  BTERR status = bt_deletekey (btree_db, key->GetData(), key->GetLength(), 0);

  printf("status :: %d \n", status);

  if(status == BTERR_ok)
    return true;

  return false;
}

bool BtreeMultimapIndex::Exists(storage::Tuple *key) const{

  std::cout << "+++++++++++++++++++++++++++ Exist key  :: \n" << (*key);

  int found = bt_findkey (btree_db, key->GetData(), key->GetLength(), nullptr, 0);

  printf("found :: %d \n", found);

  if(found == -1)
    return false;

  return true;
}

std::vector<ItemPointer> BtreeMultimapIndex::Scan() const{
  std::vector<ItemPointer> result;

  BtPageSet set[1];
  BtKey *ptr;
  BtVal *val;
  BtDb *bt = btree_db;

  int cnt = 0;
  uid next, page_no = LEAF_page;  // start on first page of leaves
  int len = 0;
  unsigned int slot;

  storage::Tuple tuple(key_schema);
  ItemPointer *item;

  std::cout << "+++++++++++++++++++++++++++ Scanning  :: \n";

  do {
    if( (set->latch = bt_pinlatch (bt, page_no, 1) ) )
      set->page = bt_mappage (bt, set->latch);
    else
      fprintf(stderr, "unable to obtain latch"), exit(1);
    bt_lockpage (bt, BtLockRead, set->latch);
    next = bt_getid (set->page->right);

    for( slot = 0; slot++ < set->page->cnt; ) {
      if( next || slot < set->page->cnt )
        if( !slotptr(set->page, slot)->dead ) {

          printf("slot %d alive \n", slot);

          ptr = keyptr(set->page, slot);
          len = ptr->len;

          if( slotptr(set->page, slot)->type == Duplicate )
            len -= BtId;

          tuple.Move(ptr->key);
          val = valptr(set->page, slot);
          item = (ItemPointer *) val->value;

          std::cout << "Tuple :: " << tuple << " block : " << item->block
              << " offset : " << item->offset << "\n";

          result.push_back(*item);
          cnt++;
        }
    }

    bt_unlockpage (bt, BtLockRead, set->latch);
    bt_unpinlatch (set->latch);
  } while( (page_no = next ) );

  tuple.Move(nullptr);

  std::cout << "Total keys read " << cnt << " reads " << bt->reads << " writes " << bt->writes << "\n";

  return result;
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



