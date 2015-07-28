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

#include "backend/index/btree_index.h"

namespace peloton {
namespace index {

BtreeMultimapIndex::BtreeMultimapIndex(IndexMetadata *metadata)
    : Index(metadata) {
  // Start manager
  size_t pool_size = 1024;
  unsigned int bits = 16;

  btree_manager = bt_mgr((char *)metadata->index_name.c_str(), bits, pool_size);

  btree_db = bt_open(btree_manager);

  btree_db->key_schema = this->GetKeySchema();

  unique_keys = metadata->unique_keys;
}

BtreeMultimapIndex::~BtreeMultimapIndex() {
  // Close
  bt_close(btree_db);

  // Close manager
  bt_mgrclose(btree_manager);
}

bool BtreeMultimapIndex::InsertEntry(const storage::Tuple *key,
                                     ItemPointer location) {
  BTERR status = bt_insertkey(btree_db, key->GetData(), key->GetLength(), 0,
                              &location, sizeof(ItemPointer), unique_keys);

  if (status == BTERR_ok) {
    return true;
  }

  std::cout << "Key :: " << (*key);
  return false;
}

bool BtreeMultimapIndex::DeleteEntry(const storage::Tuple *key) {
  BTERR status = bt_deletekey(btree_db, key->GetData(), 0, unique_keys);

  if (status == BTERR_ok) {
    return true;
  }

  return false;
}

bool BtreeMultimapIndex::Exists(const storage::Tuple *key) const {
  int found =
      bt_findkey(btree_db, key->GetData(), key->GetLength(), nullptr, 0);

  if (found == -1) {
    return false;
  }

  return true;
}

std::vector<ItemPointer> BtreeMultimapIndex::Scan() const {
  std::vector<ItemPointer> result;

  BtPageSet set[1];
  BtKey *ptr;
  BtVal *val;
  BtDb *bt = btree_db;

  int cnt = 0;
  uid next, page_no = LEAF_page;  // start on first page of leaves
  int len = 0;
  unsigned int slot;

  storage::Tuple tuple(this->GetKeySchema());
  ItemPointer *item;

  do {
    if ((set->latch = bt_pinlatch(bt, page_no, 1)))
      set->page = bt_mappage(bt, set->latch);
    else
      ::fprintf(::stderr, "unable to obtain latch"), exit(1);
    bt_lockpage(bt, BtLockRead, set->latch);
    next = bt_getid(set->page->right);

    for (slot = 0; slot++ < set->page->cnt;) {
      if (next || slot < set->page->cnt)
        if (!slotptr(set->page, slot)->dead) {
          ptr = keyptr(set->page, slot);
          len = ptr->len;

          if (slotptr(set->page, slot)->type == Duplicate) len -= BtId;

          tuple.Move(ptr->key);
          val = valptr(set->page, slot);
          item = (ItemPointer *)val->value;

          std::cout << "Tuple :: " << tuple << " block : " << item->block
                    << " offset : " << item->offset << "\n";

          result.push_back(*item);
          cnt++;
        }
    }

    bt_unlockpage(bt, BtLockRead, set->latch);
    bt_unpinlatch(set->latch);
  } while ((page_no = next));

  tuple.Move(nullptr);

  std::cout << "Total keys read " << cnt << "\n";

  return result;
}

std::vector<ItemPointer> BtreeMultimapIndex::GetLocationsForKey(
    const storage::Tuple *key) const {
  std::vector<ItemPointer> result;

  BtPageSet set[1];
  uint len, slot;
  BtKey *ptr;
  BtVal *val;
  ItemPointer *item;

  storage::Tuple tuple(this->GetKeySchema());

  if ((slot = bt_loadpage(btree_db, set, key->GetData(), 0, BtLockRead))) {
    do {
      ptr = keyptr(set->page, slot);

      //  skip librarian slot place holder

      if (slotptr(set->page, slot)->type == Librarian)
        ptr = keyptr(set->page, ++slot);

      //  return actual key found

      memcpy(btree_db->key, ptr, ptr->len + sizeof(BtKey));
      len = ptr->len;

      if (slotptr(set->page, slot)->type == Duplicate) len -= BtId;

      //  not there if we reach the stopper key

      if (slot == set->page->cnt)
        if (!bt_getid(set->page->right)) break;

      // if key exists, return >= 0 value bytes copied
      //  otherwise return (-1)

      if (slotptr(set->page, slot)->dead) continue;

      if (!keycmp(ptr, key->GetData(), this->GetKeySchema())) {
        val = valptr(set->page, slot);

        tuple.Move(ptr->key);
        item = (ItemPointer *)val->value;
        result.push_back(*item);

        std::cout << "Tuple :: " << tuple << " block : " << item->block
                  << " offset : " << item->offset << "\n";
      } else {
        break;
      }

    } while ((slot = bt_findnext(btree_db, set, slot)));
  }

  tuple.Move(nullptr);

  bt_unlockpage(btree_db, BtLockRead, set->latch);
  bt_unpinlatch(set->latch);

  return result;
}

std::vector<ItemPointer> BtreeMultimapIndex::GetLocationsForKeyBetween(
    const storage::Tuple *start, const storage::Tuple *end) const {
  std::vector<ItemPointer> result;

  BtPageSet set[1];
  uint len, slot;
  BtKey *ptr;
  BtVal *val;
  ItemPointer *item;

  storage::Tuple tuple(this->GetKeySchema());

  if ((slot = bt_loadpage(btree_db, set, start->GetData(), 0, BtLockRead))) {
    do {
      ptr = keyptr(set->page, slot);

      //  skip librarian slot place holder

      if (slotptr(set->page, slot)->type == Librarian)
        ptr = keyptr(set->page, ++slot);

      //  return actual key found

      memcpy(btree_db->key, ptr, ptr->len + sizeof(BtKey));
      len = ptr->len;

      if (slotptr(set->page, slot)->type == Duplicate) len -= BtId;

      //  not there if we reach the stopper key

      if (slot == set->page->cnt)
        if (!bt_getid(set->page->right)) break;

      // if key exists, return >= 0 value bytes copied
      //  otherwise return (-1)

      if (slotptr(set->page, slot)->dead) continue;

      //===--------------------------------------------------------------------===//
      // Comparator
      //===--------------------------------------------------------------------===//

      if (keycmp(ptr, end->GetData(), this->GetKeySchema()) <= 0) {
        val = valptr(set->page, slot);

        tuple.Move(ptr->key);
        item = (ItemPointer *)val->value;
        result.push_back(*item);

        // std::cout << "Tuple :: " << tuple << " block : " << item->block
        //    << " offset : " << item->offset << "\n";
      } else {
        break;
      }

    } while ((slot = bt_findnext(btree_db, set, slot)));
  }

  tuple.Move(nullptr);

  bt_unlockpage(btree_db, BtLockRead, set->latch);
  bt_unpinlatch(set->latch);

  return result;
}

std::vector<ItemPointer> BtreeMultimapIndex::GetLocationsForKeyLT(
    const storage::Tuple *key) const {
  std::vector<ItemPointer> result;

  BtPageSet set[1];
  BtKey *ptr;
  BtVal *val;
  BtDb *bt = btree_db;

  int cnt = 0;
  uid next, page_no = LEAF_page;  // start on first page of leaves
  int len = 0;
  unsigned int slot;

  storage::Tuple tuple(this->GetKeySchema());
  ItemPointer *item;

  do {
    if ((set->latch = bt_pinlatch(bt, page_no, 1)))
      set->page = bt_mappage(bt, set->latch);
    else
      ::fprintf(::stderr, "unable to obtain latch"), exit(1);

    bt_lockpage(bt, BtLockRead, set->latch);
    next = bt_getid(set->page->right);

    for (slot = 0; slot++ < set->page->cnt;) {
      if (next || slot < set->page->cnt)
        if (!slotptr(set->page, slot)->dead) {
          ptr = keyptr(set->page, slot);
          len = ptr->len;

          if (slotptr(set->page, slot)->type == Duplicate) len -= BtId;

          //===--------------------------------------------------------------------===//
          // Comparator
          //===--------------------------------------------------------------------===//

          if (keycmp(ptr, key->GetData(), this->GetKeySchema()) == 0) break;

          tuple.Move(ptr->key);
          val = valptr(set->page, slot);
          item = (ItemPointer *)val->value;
          result.push_back(*item);
          cnt++;
        }
    }

    bt_unlockpage(bt, BtLockRead, set->latch);
    bt_unpinlatch(set->latch);

  } while ((page_no = next));

  tuple.Move(nullptr);

  return result;
}

std::vector<ItemPointer> BtreeMultimapIndex::GetLocationsForKeyLTE(
    const storage::Tuple *key) const {
  std::vector<ItemPointer> result;

  BtPageSet set[1];
  BtKey *ptr;
  BtVal *val;
  BtDb *bt = btree_db;

  int cnt = 0;
  uid next, page_no = LEAF_page;  // start on first page of leaves
  int len = 0;
  unsigned int slot;

  storage::Tuple tuple(this->GetKeySchema());
  ItemPointer *item;

  do {
    if ((set->latch = bt_pinlatch(bt, page_no, 1)))
      set->page = bt_mappage(bt, set->latch);
    else
      ::fprintf(::stderr, "unable to obtain latch"), exit(1);

    bt_lockpage(bt, BtLockRead, set->latch);
    next = bt_getid(set->page->right);

    for (slot = 0; slot++ < set->page->cnt;) {
      if (next || slot < set->page->cnt)
        if (!slotptr(set->page, slot)->dead) {
          ptr = keyptr(set->page, slot);
          len = ptr->len;

          if (slotptr(set->page, slot)->type == Duplicate) len -= BtId;

          //===--------------------------------------------------------------------===//
          // Comparator
          //===--------------------------------------------------------------------===//

          if (keycmp(ptr, key->GetData(), this->GetKeySchema()) == 1) break;

          tuple.Move(ptr->key);
          val = valptr(set->page, slot);
          item = (ItemPointer *)val->value;
          result.push_back(*item);
          cnt++;
        }
    }

    bt_unlockpage(bt, BtLockRead, set->latch);
    bt_unpinlatch(set->latch);

  } while ((page_no = next));

  tuple.Move(nullptr);
  return result;
}

std::vector<ItemPointer> BtreeMultimapIndex::GetLocationsForKeyGT(
    const storage::Tuple *key) const {
  std::vector<ItemPointer> result;

  BtPageSet set[1];
  uint len, slot;
  BtKey *ptr;
  BtVal *val;
  ItemPointer *item;
  storage::Tuple tuple(this->GetKeySchema());

  if ((slot = bt_loadpage(btree_db, set, key->GetData(), 0, BtLockRead))) {
    do {
      ptr = keyptr(set->page, slot);

      //  skip librarian slot place holder

      if (slotptr(set->page, slot)->type == Librarian)
        ptr = keyptr(set->page, ++slot);

      //  return actual key found

      memcpy(btree_db->key, ptr, ptr->len + sizeof(BtKey));
      len = ptr->len;

      if (slotptr(set->page, slot)->type == Duplicate) len -= BtId;

      //  not there if we reach the stopper key

      if (slot == set->page->cnt)
        if (!bt_getid(set->page->right)) break;

      // if key exists, return >= 0 value bytes copied
      //  otherwise return (-1)

      if (slotptr(set->page, slot)->dead) continue;

      //===--------------------------------------------------------------------===//
      // Comparator
      //===--------------------------------------------------------------------===//

      if (keycmp(ptr, key->GetData(), this->GetKeySchema()) == 0) continue;

      val = valptr(set->page, slot);

      tuple.Move(ptr->key);
      item = (ItemPointer *)val->value;
      result.push_back(*item);

    } while ((slot = bt_findnext(btree_db, set, slot)));
  }

  tuple.Move(nullptr);

  bt_unlockpage(btree_db, BtLockRead, set->latch);
  bt_unpinlatch(set->latch);

  return result;
}

std::vector<ItemPointer> BtreeMultimapIndex::GetLocationsForKeyGTE(
    const storage::Tuple *key) const {
  std::vector<ItemPointer> result;

  BtPageSet set[1];
  uint len, slot;
  BtKey *ptr;
  BtVal *val;
  ItemPointer *item;

  storage::Tuple tuple(this->GetKeySchema());

  if ((slot = bt_loadpage(btree_db, set, key->GetData(), 0, BtLockRead))) {
    do {
      ptr = keyptr(set->page, slot);

      //  skip librarian slot place holder

      if (slotptr(set->page, slot)->type == Librarian)
        ptr = keyptr(set->page, ++slot);

      //  return actual key found

      memcpy(btree_db->key, ptr, ptr->len + sizeof(BtKey));
      len = ptr->len;

      if (slotptr(set->page, slot)->type == Duplicate) len -= BtId;

      //  not there if we reach the stopper key

      if (slot == set->page->cnt)
        if (!bt_getid(set->page->right)) break;

      // if key exists, return >= 0 value bytes copied
      //  otherwise return (-1)

      if (slotptr(set->page, slot)->dead) continue;

      //===--------------------------------------------------------------------===//
      // No Comparator
      //===--------------------------------------------------------------------===//

      val = valptr(set->page, slot);

      tuple.Move(ptr->key);
      item = (ItemPointer *)val->value;
      result.push_back(*item);
    } while ((slot = bt_findnext(btree_db, set, slot)));
  }

  tuple.Move(nullptr);

  bt_unlockpage(btree_db, BtLockRead, set->latch);
  bt_unpinlatch(set->latch);

  return result;
}

}  // End index namespace
}  // End peloton namespace
