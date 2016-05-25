//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bwtree_index.cpp
//
// Identification: src/backend/index/bwtree_index.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/common/logger.h"
#include "backend/index/bwtree_index.h"
#include "backend/index/index_key.h"
#include "backend/storage/tuple.h"

namespace peloton {
namespace index {

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
BwTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::BwTreeIndex(
    IndexMetadata *metadata)
    : Index(metadata),
      container(KeyComparator(metadata)),
      equals(metadata),
      comparator(metadata) {}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
BwTreeIndex<KeyType, ValueType, KeyComparator,
            KeyEqualityChecker>::~BwTreeIndex() {
  // Add your implementation here
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
bool BwTreeIndex<KeyType, ValueType, KeyComparator,
                 KeyEqualityChecker>::InsertEntry(UNUSED_ATTRIBUTE const
                                                  storage::Tuple *key,
                                                  UNUSED_ATTRIBUTE const
                                                  ItemPointer &location) {
  // Add your implementation here
  return false;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
bool BwTreeIndex<KeyType, ValueType, KeyComparator,
                 KeyEqualityChecker>::DeleteEntry(UNUSED_ATTRIBUTE const
                                                  storage::Tuple *key,
                                                  UNUSED_ATTRIBUTE const
                                                  ItemPointer &location) {
  // Add your implementation here
  return false;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
bool BwTreeIndex<KeyType, ValueType, KeyComparator,
                 KeyEqualityChecker>::CondInsertEntry(
    UNUSED_ATTRIBUTE const storage::Tuple *key,
    UNUSED_ATTRIBUTE const ItemPointer &location, UNUSED_ATTRIBUTE
    std::function<bool(const ItemPointer &)> predicate) {
  return false;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
void BwTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Scan(
    UNUSED_ATTRIBUTE const std::vector<Value> &values,
    UNUSED_ATTRIBUTE const std::vector<oid_t> &key_column_ids,
    UNUSED_ATTRIBUTE const std::vector<ExpressionType> &expr_types,
    UNUSED_ATTRIBUTE const ScanDirectionType &scan_direction,
    UNUSED_ATTRIBUTE std::vector<ItemPointer> &result) {}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
void
BwTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ScanAllKeys(
    UNUSED_ATTRIBUTE std::vector<ItemPointer> &result) {}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
void
BwTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ScanKey(
    UNUSED_ATTRIBUTE const storage::Tuple *key,
    UNUSED_ATTRIBUTE std::vector<ItemPointer> &result) {}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
void BwTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Scan(
    UNUSED_ATTRIBUTE const std::vector<Value> &values,
    UNUSED_ATTRIBUTE const std::vector<oid_t> &key_column_ids,
    UNUSED_ATTRIBUTE const std::vector<ExpressionType> &expr_types,
    UNUSED_ATTRIBUTE const ScanDirectionType &scan_direction,
    UNUSED_ATTRIBUTE std::vector<ItemPointer *> &result) {}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
void
BwTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ScanAllKeys(
    UNUSED_ATTRIBUTE std::vector<ItemPointer *> &result) {}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
void
BwTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ScanKey(
    UNUSED_ATTRIBUTE const storage::Tuple *key,
    UNUSED_ATTRIBUTE std::vector<ItemPointer *> &result) {}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
std::string BwTreeIndex<KeyType, ValueType, KeyComparator,
                        KeyEqualityChecker>::GetTypeName() const {
  return "BwTree";
}

// Explicit template instantiation
template class BwTreeIndex<IntsKey<1>, ItemPointer *,
                           IntsComparator<1>, IntsEqualityChecker<1>>;
template class BwTreeIndex<IntsKey<2>, ItemPointer *,
                           IntsComparator<2>, IntsEqualityChecker<2>>;
template class BwTreeIndex<IntsKey<3>, ItemPointer *,
                           IntsComparator<3>, IntsEqualityChecker<3>>;
template class BwTreeIndex<IntsKey<4>, ItemPointer *,
                           IntsComparator<4>, IntsEqualityChecker<4>>;

template class BwTreeIndex<GenericKey<4>, ItemPointer *,
                           GenericComparator<4>, GenericEqualityChecker<4>>;
template class BwTreeIndex<GenericKey<8>, ItemPointer *,
                           GenericComparator<8>, GenericEqualityChecker<8>>;
template class BwTreeIndex<GenericKey<12>, ItemPointer *,
                           GenericComparator<12>, GenericEqualityChecker<12>>;
template class BwTreeIndex<GenericKey<16>, ItemPointer *,
                           GenericComparator<16>, GenericEqualityChecker<16>>;
template class BwTreeIndex<GenericKey<24>, ItemPointer *,
                           GenericComparator<24>, GenericEqualityChecker<24>>;
template class BwTreeIndex<GenericKey<32>, ItemPointer *,
                           GenericComparator<32>, GenericEqualityChecker<32>>;
template class BwTreeIndex<GenericKey<48>, ItemPointer *,
                           GenericComparator<48>, GenericEqualityChecker<48>>;
template class BwTreeIndex<GenericKey<64>, ItemPointer *,
                           GenericComparator<64>, GenericEqualityChecker<64>>;
template class BwTreeIndex<GenericKey<96>, ItemPointer *,
                           GenericComparator<96>, GenericEqualityChecker<96>>;
template class BwTreeIndex<GenericKey<128>, ItemPointer *,
                           GenericComparator<128>, GenericEqualityChecker<128>>;
template class BwTreeIndex<GenericKey<256>, ItemPointer *,
                           GenericComparator<256>, GenericEqualityChecker<256>>;
template class BwTreeIndex<GenericKey<512>, ItemPointer *,
                           GenericComparator<512>, GenericEqualityChecker<512>>;

template class BwTreeIndex<TupleKey, ItemPointer *, TupleKeyComparator,
                           TupleKeyEqualityChecker>;

}  // End index namespace
}  // End peloton namespace