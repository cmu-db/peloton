/*-------------------------------------------------------------------------
 *
 * array_unique_index.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 *-------------------------------------------------------------------------
 */

#include "index/array_unique_index.h"

#include "storage/tile.h"
#include "common/value.h"
#include "common/value_peeker.h"

#include <sstream>
#include <cstdlib>
#include <map>

namespace nstore {
namespace index {

ArrayUniqueIndex::ArrayUniqueIndex(const IndexMetadata &metadata) : Index(metadata) {

  assert(column_count == 1);

  catalog::ColumnInfo column_info = key_schema->GetColumnInfo(0);

  assert(( column_info.type == VALUE_TYPE_TINYINT) || (column_info.type == VALUE_TYPE_SMALLINT) ||
         ( column_info.type == VALUE_TYPE_INTEGER) || ( column_info.type == VALUE_TYPE_BIGINT));

  tile_column_id = metadata.table_columns_in_key[0];

  entries = new void*[ARRAY_INDEX_INITIAL_SIZE];
  ::memset(entries, 0, sizeof(void*) * ARRAY_INDEX_INITIAL_SIZE);

  allocated_entry_count = ARRAY_INDEX_INITIAL_SIZE;
  cursor = -1;
}

ArrayUniqueIndex::~ArrayUniqueIndex() {

  // clean up entries
  delete[] entries;

}

bool ArrayUniqueIndex::AddEntry(const ItemPointer *tuple) {

  storage::Tuple *tile_tuple = storage::Tile::GetTuple(catalog, tuple);

  const int32_t key = ValuePeeker::PeekAsInteger(tile_tuple->GetValue(tile_column_id));
  assert((key < ARRAY_INDEX_INITIAL_SIZE) && (key >= 0));

  // uniqueness check
  if (entries[key] != NULL)
    return false;

  entries[key] = (ItemPointer*) tuple;

  ++insert_counter;
  return true;
}

bool ArrayUniqueIndex::DeleteEntry(const ItemPointer *tuple) {

  storage::Tuple *tile_tuple = storage::Tile::GetTuple(catalog, tuple);

  const int32_t key = ValuePeeker::PeekAsInteger(tile_tuple->GetValue(tile_column_id));
  assert((key < ARRAY_INDEX_INITIAL_SIZE) && (key >= 0));

  entries[key] = NULL;
  ++delete_counter;
  return true; //deleted
}

bool ArrayUniqueIndex::UpdateEntry(const ItemPointer *old_tuple, const ItemPointer* new_tuple) {

  storage::Tuple *old_tile_tuple = storage::Tile::GetTuple(catalog, old_tuple);
  storage::Tuple *new_tile_tuple = storage::Tile::GetTuple(catalog, new_tuple);

  // this can probably be optimized
  int32_t old_key = ValuePeeker::PeekAsInteger(old_tile_tuple->GetValue(tile_column_id));
  int32_t new_key = ValuePeeker::PeekAsInteger(new_tile_tuple->GetValue(tile_column_id));

  assert((old_key < ARRAY_INDEX_INITIAL_SIZE) && (old_key >= 0));
  assert((new_key < ARRAY_INDEX_INITIAL_SIZE) && (new_key >= 0));

  // no update is needed for this index
  if (old_key == new_key)
    return true;

  entries[new_key] = (ItemPointer*) new_tuple;
  entries[old_key] = NULL;

  ++update_counter;
  return true;
}

bool ArrayUniqueIndex::SetValue(const ItemPointer *tuple, const void* address) {

  storage::Tuple *tile_tuple = storage::Tile::GetTuple(catalog, tuple);

  int32_t key = ValuePeeker::PeekAsInteger(tile_tuple->GetValue(tile_column_id));

  assert(key < ARRAY_INDEX_INITIAL_SIZE && key >= 0);

  //memcpy(entries[key], address, sizeof(void*)); // HACK to get around constness

  entries[key] = (void *) address;
  ++update_counter;

  return true;
}

bool ArrayUniqueIndex::Exists(const ItemPointer* tuple) {

  storage::Tuple *tile_tuple = storage::Tile::GetTuple(catalog, tuple);

  int32_t key = ValuePeeker::PeekAsInteger(tile_tuple->GetValue(tile_column_id));
  assert(key < ARRAY_INDEX_INITIAL_SIZE && key >= 0);

  if (key >= allocated_entry_count)
    return false;

  ++lookup_counter;

  return entries[key] != NULL;
}

bool ArrayUniqueIndex::MoveToKey(const ItemPointer *search_key) {
  storage::Tuple *tile_tuple = storage::Tile::GetTuple(catalog, search_key);
  cursor = ValuePeeker::PeekAsInteger(tile_tuple->GetValue(tile_column_id));

  if (cursor < 0)
    return false;
  assert(cursor < ARRAY_INDEX_INITIAL_SIZE);

  ++lookup_counter;
  return true;
}

bool ArrayUniqueIndex::MoveToTuple(const ItemPointer *search_tuple) {
  storage::Tuple *tile_tuple = storage::Tile::GetTuple(catalog, search_tuple);
  cursor = ValuePeeker::PeekAsInteger(tile_tuple->GetValue(tile_column_id));

  if (cursor < 0)
    return false;
  assert(cursor < ARRAY_INDEX_INITIAL_SIZE);

  ++lookup_counter;
  return true;
}

storage::Tuple *ArrayUniqueIndex::NextValueAtKey() {
  if (cursor == -1)
    return nullptr;

  if (entries[cursor] == NULL)
    return nullptr;

  storage::Tuple *tuple = storage::Tile::GetTuple(catalog, (ItemPointer *) entries[cursor]);

  // unique index, so can set to -1
  cursor = -1;

  return tuple;
}

bool ArrayUniqueIndex::AdvanceToNextKey() {
  assert((cursor < ARRAY_INDEX_INITIAL_SIZE) && (cursor >= 0));

  ++cursor;
  return true;
}

bool ArrayUniqueIndex::CheckForIndexChange(const ItemPointer *lhs, const ItemPointer *rhs) {

  storage::Tuple *lhs_tile_tuple = storage::Tile::GetTuple(catalog, lhs);
  storage::Tuple *rhs_tile_tuple = storage::Tile::GetTuple(catalog, rhs);

  return (lhs_tile_tuple->GetValue(tile_column_id).OpEquals(rhs_tile_tuple->GetValue(tile_column_id)).IsFalse());
}

} // End index namespace
} // End nstore namespace

