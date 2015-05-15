/*-------------------------------------------------------------------------
 *
 * array_unique_index.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/index/array_unique_index.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "index/array_unique_index.h"

#include "common/logger.h"
#include "common/value.h"
#include "common/value_peeker.h"
#include "common/types.h"
#include "storage/tuple.h"

#include <sstream>
#include <map>
#include <cstdlib>

namespace nstore {
namespace index {

ArrayUniqueIndex::ArrayUniqueIndex(const TableIndexScheme &scheme) : TableIndex(scheme) {
  assert(colCount_ == 1);
  assert((column_types_[0] == VALUE_TYPE_TINYINT) || (column_types_[0] == VALUE_TYPE_SMALLINT) ||
         (column_types_[0] == VALUE_TYPE_INTEGER) || (column_types_[0] == VALUE_TYPE_BIGINT));
  entries_ = new void*[ARRAY_INDEX_INITIAL_SIZE];
  ::memset(entries_, 0, sizeof(void*) * ARRAY_INDEX_INITIAL_SIZE);
  allocated_entries_ = ARRAY_INDEX_INITIAL_SIZE;
  match_i_ = 0;
}

ArrayUniqueIndex::~ArrayUniqueIndex() {
  delete[] entries_;
}

bool ArrayUniqueIndex::addEntry(const storage::Tuple *tuple) {
  const int32_t key = ValuePeeker::PeekAsInteger(tuple->GetValue(column_indices_[0]));
  //VOLT_TRACE ("Adding entry %ld from column index %d", key, column_indices_[0]);
  assert((key < ARRAY_INDEX_INITIAL_SIZE) && (key >= 0));

  // uniqueness check
  if (entries_[key] != NULL)
    return false;
  entries_[key] = static_cast<void*>(const_cast<storage::Tuple*>(tuple)->GetData());
  ++m_inserts;
  return true;
}

bool ArrayUniqueIndex::deleteEntry(const storage::Tuple *tuple) {
  const int32_t key = ValuePeeker::PeekAsInteger(tuple->GetValue(column_indices_[0]));
  assert((key < ARRAY_INDEX_INITIAL_SIZE) && (key >= 0));

  //VOLT_DEBUG("Deleting entry %lld", key);
  entries_[key] = NULL;
  ++m_deletes;
  return true; //deleted
}

bool ArrayUniqueIndex::replaceEntry(const storage::Tuple *oldTupleValue, const storage::Tuple* newTupleValue) {
  // this can probably be optimized
  int32_t old_key = ValuePeeker::PeekAsInteger(oldTupleValue->GetValue(column_indices_[0]));
  int32_t new_key = ValuePeeker::PeekAsInteger(newTupleValue->GetValue(column_indices_[0]));
  assert((old_key < ARRAY_INDEX_INITIAL_SIZE) && (old_key >= 0));
  assert((new_key < ARRAY_INDEX_INITIAL_SIZE) && (new_key >= 0));
  if (old_key == new_key) return true; // no update is needed for this index

  entries_[new_key] = const_cast<storage::Tuple*>(newTupleValue)->GetData();
  entries_[old_key] = NULL;
  ++m_updates;
  return true;
}

bool ArrayUniqueIndex::setEntryToNewAddress(const storage::Tuple *tuple, const void* address)
{
  int32_t key = ValuePeeker::PeekAsInteger(tuple->GetValue(column_indices_[0]));

  assert(key < ARRAY_INDEX_INITIAL_SIZE);
  assert(key >= 0);

  memcpy(entries_[key], address, sizeof(void*)); // HACK to get around constness

  //entries_[key] = address;
  ++m_updates;

  return true;
}

bool ArrayUniqueIndex::exists(const storage::Tuple* values) {
  int32_t key = ValuePeeker::PeekAsInteger(values->GetValue(column_indices_[0]));
  //VOLT_DEBUG("Exists?: %lld", key);
  assert(key < ARRAY_INDEX_INITIAL_SIZE);
  assert(key >= 0);
  if (key >= allocated_entries_) return false;
  LOG_TRACE("Checking entry b: %d", (int)key);
  ++m_lookups;
  return entries_[key] != NULL;
}

bool ArrayUniqueIndex::moveToKey(const storage::Tuple *searchKey) {
  match_i_ = ValuePeeker::PeekAsInteger(searchKey->GetValue(0));
  if (match_i_ < 0) return false;
  assert(match_i_ < ARRAY_INDEX_INITIAL_SIZE);
  ++m_lookups;
  return entries_[match_i_];
}

bool ArrayUniqueIndex::moveToTuple(const storage::Tuple *searchTuple) {
  match_i_ = ValuePeeker::PeekAsInteger(searchTuple->GetValue(0));
  if (match_i_ < 0) return false;
  assert(match_i_ < ARRAY_INDEX_INITIAL_SIZE);
  ++m_lookups;
  return entries_[match_i_];
}

storage::Tuple ArrayUniqueIndex::nextValueAtKey() {
  if (match_i_ == -1) return storage::Tuple();
  if (!(entries_[match_i_])) return storage::Tuple();
  storage::Tuple retval(m_tupleSchema, entries_[match_i_]);
  match_i_ = -1;
  return retval;
}

bool ArrayUniqueIndex::advanceToNextKey() {
  assert((match_i_ < ARRAY_INDEX_INITIAL_SIZE) && (match_i_ >= 0));
  return entries_[++match_i_];
}

bool ArrayUniqueIndex::checkForIndexChange(const storage::Tuple *lhs, const storage::Tuple *rhs) {
  return lhs->GetValue(column_indices_[0]).OpNotEquals(rhs->GetValue(column_indices_[0])).IsTrue();
}

} // End index namespace
} // End nstore namespace

