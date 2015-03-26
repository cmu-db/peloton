/*-------------------------------------------------------------------------
 *
 * array_unique_index.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 *-------------------------------------------------------------------------
 */

#include "common/types.h"
#include "catalog/catalog.h"
#include "storage/tuple.h"
#include "index/index.h"

#include <vector>
#include <string>
#include <map>

namespace nstore {
namespace index {

const int ARRAY_INDEX_INITIAL_SIZE = 131072; // (2^17)

/**
 * Unique Index specialized for 1 integer.
 *
 * This is implemented as a giant array, which gives optimal performance as far
 * as the entry value is assured to be sequential and limited to a small number.
 *
 * @see Index
 */
class ArrayUniqueIndex : public Index {
  friend class IndexFactory;

 public:
  ~ArrayUniqueIndex();

  bool AddEntry(const ItemPointer *tuple);

  bool DeleteEntry(const ItemPointer *tuple);

  bool UpdateEntry(const ItemPointer *old_tuple, const ItemPointer* new_tuple);

  bool SetValue(const ItemPointer *tuple, const void* address);

  bool Exists(const ItemPointer* tuple);

  bool MoveToKey(const ItemPointer *search_key);

  bool MoveToTuple(const ItemPointer *search_tuple);

  storage::Tuple *NextValueAtKey();

  bool AdvanceToNextKey();

  bool CheckForIndexChange(const ItemPointer *lhs, const ItemPointer *rhs);

  size_t GetSize() const {
    return 0;
  }

  int64_t GetMemoryEstimate() const {
    return 0;
  }

  std::string GetTypeName() const {
    return "ArrayIntUniqueIndex";
  }

 protected:
  ArrayUniqueIndex(const IndexMetadata &metadata);

  void **entries;

  // mapping from key to tile
  id_t tile_column_id;

  int32_t allocated_entry_count;

  int32_t cursor;
};

} // End index namespace
} // End nstore namespace

