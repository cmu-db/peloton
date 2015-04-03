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

  std::cout << "+++++++++++++++++++++++++++ Insert key  :: " << (*key);

  BTERR status = bt_insertkey (btree_db, key->GetData(), key->GetLength(), 0, &location, sizeof(ItemPointer), 0);

  if(status == BTERR_ok) {
    std::cout << "Inserted \n";
    return true;
  }

  std::cout << "Did not insert \n";
  return false;
}

bool BtreeMultimapIndex::DeleteEntry(storage::Tuple *key){

  std::cout << "+++++++++++++++++++++++++++ Delete key  :: " << (*key);

  BTERR status = bt_deletekey (btree_db, key->GetData(), 0, 0);

  if(status == BTERR_ok) {
    std::cout << "Deleted \n";
    return true;
  }

  std::cout << "Did not delete\n";
  return false;
}

bool BtreeMultimapIndex::Exists(storage::Tuple *key) const{

  std::cout << "+++++++++++++++++++++++++++ Exist key  :: " << (*key);

  int found = bt_findkey (btree_db, key->GetData(), key->GetLength(), nullptr, 0);

  if(found == -1) {
    std::cout << "Does not exist \n";
    return false;
  }

  std::cout << "Exists \n";

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

  std::cout << "Total keys read " << cnt << "\n";

  return result;
}

std::vector<ItemPointer> BtreeMultimapIndex::GetLocationsForKey(storage::Tuple *key) const{
  std::vector<ItemPointer> result;

  BtPageSet set[1];
  uint len, slot;
  BtKey *ptr;
  BtVal *val;
  ItemPointer *item;

  storage::Tuple tuple(key_schema);

  std::cout << "+++++++++++++++++++++++++++ Get Locations For Key  :: \n";

  if( (slot = bt_loadpage (btree_db, set, key->GetData(), 0, BtLockRead)) ) {

    do {
      ptr = keyptr(set->page, slot);

      //  skip librarian slot place holder

      if( slotptr(set->page, slot)->type == Librarian )
        ptr = keyptr(set->page, ++slot);

      //  return actual key found

      memcpy (btree_db->key, ptr, ptr->len + sizeof(BtKey));
      len = ptr->len;

      if( slotptr(set->page, slot)->type == Duplicate )
        len -= BtId;

      //  not there if we reach the stopper key

      if( slot == set->page->cnt )
        if( !bt_getid (set->page->right) )
          break;

      // if key exists, return >= 0 value bytes copied
      //  otherwise return (-1)

      if( slotptr(set->page, slot)->dead )
        continue;

      if( !keycmp (ptr, key->GetData(), key_schema) ) {
        val = valptr (set->page,slot);

        tuple.Move(ptr->key);
        item = (ItemPointer *) val->value;
        result.push_back(*item);

        std::cout << "Tuple :: " << tuple << " block : " << item->block
            << " offset : " << item->offset << "\n";
      }
      else {
        break;
      }

    } while( (slot = bt_findnext (btree_db, set, slot) ));

  }

  tuple.Move(nullptr);

  bt_unlockpage (btree_db, BtLockRead, set->latch);
  bt_unpinlatch (set->latch);

  return result;
}

std::vector<ItemPointer> BtreeMultimapIndex::GetLocationsForKeyBetween(storage::Tuple *start, storage::Tuple *end) const{
  std::vector<ItemPointer> result;

  BtPageSet set[1];
  uint len, slot;
  BtKey *ptr;
  BtVal *val;
  ItemPointer *item;

  storage::Tuple tuple(key_schema);

  std::cout << "+++++++++++++++++++++++++++ Get Locations For Key Between :: :: \n";

  if( (slot = bt_loadpage (btree_db, set, start->GetData(), 0, BtLockRead)) ) {

    do {
      ptr = keyptr(set->page, slot);

      //  skip librarian slot place holder

      if( slotptr(set->page, slot)->type == Librarian )
        ptr = keyptr(set->page, ++slot);

      //  return actual key found

      memcpy (btree_db->key, ptr, ptr->len + sizeof(BtKey));
      len = ptr->len;

      if( slotptr(set->page, slot)->type == Duplicate )
        len -= BtId;

      //  not there if we reach the stopper key

      if( slot == set->page->cnt )
        if( !bt_getid (set->page->right) )
          break;

      // if key exists, return >= 0 value bytes copied
      //  otherwise return (-1)

      if( slotptr(set->page, slot)->dead )
        continue;

      //===--------------------------------------------------------------------===//
      // Comparator
      //===--------------------------------------------------------------------===//

      if( keycmp (ptr, end->GetData(), key_schema) <= 0 ) {
        val = valptr (set->page,slot);

        tuple.Move(ptr->key);
        item = (ItemPointer *) val->value;
        result.push_back(*item);

        std::cout << "Tuple :: " << tuple << " block : " << item->block
            << " offset : " << item->offset << "\n";
      }
      else {
        break;
      }

    } while( (slot = bt_findnext (btree_db, set, slot) ));

  }

  tuple.Move(nullptr);

  bt_unlockpage (btree_db, BtLockRead, set->latch);
  bt_unpinlatch (set->latch);

  return result;
}

std::vector<ItemPointer> BtreeMultimapIndex::GetLocationsForKeyLT(storage::Tuple *key) const{
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

  std::cout << "+++++++++++++++++++++++++++ GetLocationsForKey Less Than  :: \n";

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

          ptr = keyptr(set->page, slot);
          len = ptr->len;

          if( slotptr(set->page, slot)->type == Duplicate )
            len -= BtId;

          //===--------------------------------------------------------------------===//
          // Comparator
          //===--------------------------------------------------------------------===//

          if( keycmp (ptr, key->GetData(), key_schema) == 0 )
            break;

          tuple.Move(ptr->key);
          val = valptr(set->page, slot);
          item = (ItemPointer *) val->value;
          result.push_back(*item);

          std::cout << "Tuple :: " << tuple << " block : " << item->block
              << " offset : " << item->offset << "\n";

          cnt++;
        }
    }

    bt_unlockpage (bt, BtLockRead, set->latch);
    bt_unpinlatch (set->latch);

  } while( (page_no = next ) );

  tuple.Move(nullptr);

  return result;
}

std::vector<ItemPointer> BtreeMultimapIndex::GetLocationsForKeyLTE(storage::Tuple *key) const{
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

  std::cout << "+++++++++++++++++++++++++++ GetLocationsForKey Less Than Or Equal To :: \n";

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

          ptr = keyptr(set->page, slot);
          len = ptr->len;

          if( slotptr(set->page, slot)->type == Duplicate )
            len -= BtId;

          //===--------------------------------------------------------------------===//
          // Comparator
          //===--------------------------------------------------------------------===//

          if( keycmp (ptr, key->GetData(), key_schema) == 1 )
            break;

          tuple.Move(ptr->key);
          val = valptr(set->page, slot);
          item = (ItemPointer *) val->value;
          result.push_back(*item);

          std::cout << "Tuple :: " << tuple << " block : " << item->block
              << " offset : " << item->offset << "\n";

          cnt++;
        }
    }

    bt_unlockpage (bt, BtLockRead, set->latch);
    bt_unpinlatch (set->latch);

  } while( (page_no = next ) );

  tuple.Move(nullptr);
  return result;
}

std::vector<ItemPointer> BtreeMultimapIndex::GetLocationsForKeyGT(storage::Tuple *key) const{
  std::vector<ItemPointer> result;

  BtPageSet set[1];
  uint len, slot;
  BtKey *ptr;
  BtVal *val;
  ItemPointer *item;

  storage::Tuple tuple(key_schema);

  std::cout << "+++++++++++++++++++++++++++ Get Locations For Key GT :: \n";

  if( (slot = bt_loadpage (btree_db, set, key->GetData(), 0, BtLockRead)) ) {

    do {
      ptr = keyptr(set->page, slot);

      //  skip librarian slot place holder

      if( slotptr(set->page, slot)->type == Librarian )
        ptr = keyptr(set->page, ++slot);

      //  return actual key found

      memcpy (btree_db->key, ptr, ptr->len + sizeof(BtKey));
      len = ptr->len;

      if( slotptr(set->page, slot)->type == Duplicate )
        len -= BtId;

      //  not there if we reach the stopper key

      if( slot == set->page->cnt )
        if( !bt_getid (set->page->right) )
          break;

      // if key exists, return >= 0 value bytes copied
      //  otherwise return (-1)

      if( slotptr(set->page, slot)->dead )
        continue;

      //===--------------------------------------------------------------------===//
      // Comparator
      //===--------------------------------------------------------------------===//

      if( keycmp (ptr, key->GetData(), key_schema) == 0 )
        continue;

      val = valptr (set->page,slot);

      tuple.Move(ptr->key);
      item = (ItemPointer *) val->value;
      result.push_back(*item);

      std::cout << "Tuple :: " << tuple << " block : " << item->block
          << " offset : " << item->offset << "\n";

    } while( (slot = bt_findnext (btree_db, set, slot) ));

  }

  tuple.Move(nullptr);

  bt_unlockpage (btree_db, BtLockRead, set->latch);
  bt_unpinlatch (set->latch);

  return result;
}

std::vector<ItemPointer> BtreeMultimapIndex::GetLocationsForKeyGTE(storage::Tuple *key) const{
  std::vector<ItemPointer> result;

  BtPageSet set[1];
    uint len, slot;
    BtKey *ptr;
    BtVal *val;
    ItemPointer *item;

    storage::Tuple tuple(key_schema);

    std::cout << "+++++++++++++++++++++++++++ Get Locations For Key GTE :: \n";

    if( (slot = bt_loadpage (btree_db, set, key->GetData(), 0, BtLockRead)) ) {

      do {
        ptr = keyptr(set->page, slot);

        //  skip librarian slot place holder

        if( slotptr(set->page, slot)->type == Librarian )
          ptr = keyptr(set->page, ++slot);

        //  return actual key found

        memcpy (btree_db->key, ptr, ptr->len + sizeof(BtKey));
        len = ptr->len;

        if( slotptr(set->page, slot)->type == Duplicate )
          len -= BtId;

        //  not there if we reach the stopper key

        if( slot == set->page->cnt )
          if( !bt_getid (set->page->right) )
            break;

        // if key exists, return >= 0 value bytes copied
        //  otherwise return (-1)

        if( slotptr(set->page, slot)->dead )
          continue;

        //===--------------------------------------------------------------------===//
        // No Comparator
        //===--------------------------------------------------------------------===//

        val = valptr (set->page,slot);

        tuple.Move(ptr->key);
        item = (ItemPointer *) val->value;
        result.push_back(*item);

        std::cout << "Tuple :: " << tuple << " block : " << item->block
            << " offset : " << item->offset << "\n";

      } while( (slot = bt_findnext (btree_db, set, slot) ));

    }

    tuple.Move(nullptr);

    bt_unlockpage (btree_db, BtLockRead, set->latch);
    bt_unpinlatch (set->latch);


  return result;
}

} // End index namespace
} // End nstore namespace



