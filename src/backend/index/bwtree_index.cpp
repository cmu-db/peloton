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
BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::BWTreeIndex(
    IndexMetadata *metadata)
    : Index(metadata), equals(metadata), comparator(metadata) {
  // Add your implementation here
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
BWTreeIndex<KeyType, ValueType, KeyComparator,
            KeyEqualityChecker>::~BWTreeIndex() {
  // Add your implementation here
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
bool BWTreeIndex<KeyType, ValueType, KeyComparator,
                 KeyEqualityChecker>::InsertEntry(__attribute__((unused)) const
                                                  storage::Tuple *key,
                                                  __attribute__((unused)) const
                                                  ItemPointer &location) {
  // Add your implementation here
  return false;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
bool BWTreeIndex<KeyType, ValueType, KeyComparator,
                 KeyEqualityChecker>::DeleteEntry(__attribute__((unused)) const
                                                  storage::Tuple *key,
                                                  __attribute__((unused)) const
                                                  ItemPointer &location) {
  // Add your implementation here
  return false;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
bool BWTreeIndex<KeyType, ValueType, KeyComparator,
                 KeyEqualityChecker>::CondInsertEntry(
    __attribute__((unused)) const storage::Tuple *key,
    __attribute__((unused)) const ItemPointer &location, __attribute__((unused))
    std::function<bool(const ItemPointer &)> predicate) {
  return false;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
void BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Scan(
    __attribute__((unused)) const std::vector<Value> &values,
    __attribute__((unused)) const std::vector<oid_t> &key_column_ids,
    __attribute__((unused)) const std::vector<ExpressionType> &expr_types,
    __attribute__((unused)) const ScanDirectionType &scan_direction,
    __attribute__((unused)) std::vector<ItemPointer> &result) {}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
void
BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ScanAllKeys(
    __attribute__((unused)) std::vector<ItemPointer> &result) {}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
void
BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ScanKey(
    __attribute__((unused)) const storage::Tuple *key,
    __attribute__((unused)) std::vector<ItemPointer> &result) {}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
void BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Scan(
    __attribute__((unused)) const std::vector<Value> &values,
    __attribute__((unused)) const std::vector<oid_t> &key_column_ids,
    __attribute__((unused)) const std::vector<ExpressionType> &expr_types,
    __attribute__((unused)) const ScanDirectionType &scan_direction,
    __attribute__((unused)) std::vector<ItemPointerContainer *> &result) {}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
void
BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ScanAllKeys(
    __attribute__((unused)) std::vector<ItemPointerContainer *> &result) {}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
void
BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ScanKey(
    __attribute__((unused)) const storage::Tuple *key,
    __attribute__((unused)) std::vector<ItemPointerContainer *> &result) {}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
std::string BWTreeIndex<KeyType, ValueType, KeyComparator,
                        KeyEqualityChecker>::GetTypeName() const {
  return "BWTree";
}

// Explicit template instantiation
template class BWTreeIndex<IntsKey<1>, ItemPointerContainer *,
                           IntsComparator<1>, IntsEqualityChecker<1>>;
template class BWTreeIndex<IntsKey<2>, ItemPointerContainer *,
                           IntsComparator<2>, IntsEqualityChecker<2>>;
template class BWTreeIndex<IntsKey<3>, ItemPointerContainer *,
                           IntsComparator<3>, IntsEqualityChecker<3>>;
template class BWTreeIndex<IntsKey<4>, ItemPointerContainer *,
                           IntsComparator<4>, IntsEqualityChecker<4>>;

template class BWTreeIndex<GenericKey<4>, ItemPointerContainer *,
                           GenericComparator<4>, GenericEqualityChecker<4>>;
template class BWTreeIndex<GenericKey<8>, ItemPointerContainer *,
                           GenericComparator<8>, GenericEqualityChecker<8>>;
template class BWTreeIndex<GenericKey<12>, ItemPointerContainer *,
                           GenericComparator<12>, GenericEqualityChecker<12>>;
template class BWTreeIndex<GenericKey<16>, ItemPointerContainer *,
                           GenericComparator<16>, GenericEqualityChecker<16>>;
template class BWTreeIndex<GenericKey<24>, ItemPointerContainer *,
                           GenericComparator<24>, GenericEqualityChecker<24>>;
template class BWTreeIndex<GenericKey<32>, ItemPointerContainer *,
                           GenericComparator<32>, GenericEqualityChecker<32>>;
template class BWTreeIndex<GenericKey<48>, ItemPointerContainer *,
                           GenericComparator<48>, GenericEqualityChecker<48>>;
template class BWTreeIndex<GenericKey<64>, ItemPointerContainer *,
                           GenericComparator<64>, GenericEqualityChecker<64>>;
template class BWTreeIndex<GenericKey<96>, ItemPointerContainer *,
                           GenericComparator<96>, GenericEqualityChecker<96>>;
template class BWTreeIndex<GenericKey<128>, ItemPointerContainer *,
                           GenericComparator<128>, GenericEqualityChecker<128>>;
template class BWTreeIndex<GenericKey<256>, ItemPointerContainer *,
                           GenericComparator<256>, GenericEqualityChecker<256>>;
template class BWTreeIndex<GenericKey<512>, ItemPointerContainer *,
                           GenericComparator<512>, GenericEqualityChecker<512>>;

template class BWTreeIndex<TupleKey, ItemPointerContainer *, TupleKeyComparator,
                           TupleKeyEqualityChecker>;

}  // End index namespace
}  // End peloton namespace