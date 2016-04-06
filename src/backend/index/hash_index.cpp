//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_index.cpp
//
// Identification: src/backend/index/hash_index.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/index/hash_index.h"
#include "backend/index/index_key.h"
#include "backend/common/logger.h"
#include "backend/storage/tuple.h"
#include <vector>

namespace peloton {
namespace index {

template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
HashIndex<KeyType, ValueType, KeyHasher, KeyComparator,
          KeyEqualityChecker>::HashIndex(IndexMetadata *metadata)
    : Index(metadata),
      container(KeyHasher(metadata), KeyEqualityChecker(metadata)),
      hasher(metadata),
      equals(metadata),
      comparator(metadata) {}

struct ItemPointerEqualityChecker {
  ItemPointer arg_;
  ItemPointerEqualityChecker(ItemPointer arg) : arg_(arg) {}
  bool operator()(ItemPointer x) {
    return x.block == arg_.block && x.offset == arg_.offset;
  }
};

template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
HashIndex<KeyType, ValueType, KeyHasher, KeyComparator,
          KeyEqualityChecker>::~HashIndex() {}

void insert_helper(std::vector<ItemPointer> &entries, void *arg) {
  ItemPointer *arg_val = (ItemPointer *)arg;
  entries.push_back(*arg_val);
}

template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
bool HashIndex<KeyType, ValueType, KeyHasher, KeyComparator,
               KeyEqualityChecker>::InsertEntry(const storage::Tuple *key,
                                                ItemPointer &location) {
  KeyType index_key;
  index_key.SetFromKey(key);

  std::vector<ValueType> val;
  val.push_back(location);
  container.upsert(index_key, insert_helper, (void *)&location, val);

  return true;
}

void delete_helper(std::vector<ItemPointer> &entries, void *arg) {
  ItemPointer *arg_val = (ItemPointer *)arg;
  entries.erase(std::remove_if(entries.begin(), entries.end(),
                               ItemPointerEqualityChecker(*arg_val)),
                entries.end());
}

template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
bool HashIndex<KeyType, ValueType, KeyHasher, KeyComparator,
               KeyEqualityChecker>::DeleteEntry(const storage::Tuple *key,
                                                const ItemPointer &location) {
  KeyType index_key;
  index_key.SetFromKey(key);
  LOG_DEBUG("location block: %lu offset: %lu", location.block, location.offset);

  // TODO: add retry logic
  container.update_fn(index_key, delete_helper, (void *)&location);

  return true;
}


template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
std::vector<ItemPointer>
HashIndex<KeyType, ValueType, KeyHasher, KeyComparator, KeyEqualityChecker>::
    Scan(__attribute__((unused)) const std::vector<Value> &values,
         __attribute__((unused)) const std::vector<oid_t> &key_column_ids,
         __attribute__((unused)) const std::vector<ExpressionType> &expr_types,
         __attribute__((unused)) const ScanDirectionType &scan_direction) {
  throw IndexException("Range query in hash index is not supported");
}

template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
std::vector<ItemPointer> HashIndex<KeyType, ValueType, KeyHasher, KeyComparator,
                                   KeyEqualityChecker>::ScanAllKeys() {
  std::vector<ItemPointer> result;
  {
    auto lt = container.lock_table();
    for (const auto &itr : lt) {
      auto entries = itr.second;
      for (const auto entry : entries) {
        result.push_back(entry);
      }
    }
  }

  return result;
}

/**
 * @brief Return all locations related to this key.
 */
template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
std::vector<ItemPointer>
HashIndex<KeyType, ValueType, KeyHasher, KeyComparator,
          KeyEqualityChecker>::ScanKey(const storage::Tuple *key) {
  KeyType index_key;
  index_key.SetFromKey(key);

  // find the <key, location> pair
  try {
    return container.find(index_key);
  } catch (const std::out_of_range &e) {
    LOG_TRACE("Key not found");
    std::vector<ItemPointer> empty_vec;
    return empty_vec;
  }
}

template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
std::string HashIndex<KeyType, ValueType, KeyHasher, KeyComparator,
                      KeyEqualityChecker>::GetTypeName() const {
  return "Hash";
}

// Explicit template instantiation
template class HashIndex<IntsKey<1>, ItemPointer, IntsHasher<1>,
                         IntsComparator<1>, IntsEqualityChecker<1>>;
template class HashIndex<IntsKey<2>, ItemPointer, IntsHasher<2>,
                         IntsComparator<2>, IntsEqualityChecker<2>>;
template class HashIndex<IntsKey<3>, ItemPointer, IntsHasher<3>,
                         IntsComparator<3>, IntsEqualityChecker<3>>;
template class HashIndex<IntsKey<4>, ItemPointer, IntsHasher<4>,
                         IntsComparator<4>, IntsEqualityChecker<4>>;

template class HashIndex<GenericKey<4>, ItemPointer, GenericHasher<4>,
                         GenericComparator<4>, GenericEqualityChecker<4>>;
template class HashIndex<GenericKey<8>, ItemPointer, GenericHasher<8>,
                         GenericComparator<8>, GenericEqualityChecker<8>>;
template class HashIndex<GenericKey<12>, ItemPointer, GenericHasher<12>,
                         GenericComparator<12>, GenericEqualityChecker<12>>;
template class HashIndex<GenericKey<16>, ItemPointer, GenericHasher<16>,
                         GenericComparator<16>, GenericEqualityChecker<16>>;
template class HashIndex<GenericKey<24>, ItemPointer, GenericHasher<24>,
                         GenericComparator<24>, GenericEqualityChecker<24>>;
template class HashIndex<GenericKey<32>, ItemPointer, GenericHasher<32>,
                         GenericComparator<32>, GenericEqualityChecker<32>>;
template class HashIndex<GenericKey<48>, ItemPointer, GenericHasher<48>,
                         GenericComparator<48>, GenericEqualityChecker<48>>;
template class HashIndex<GenericKey<64>, ItemPointer, GenericHasher<64>,
                         GenericComparator<64>, GenericEqualityChecker<64>>;
template class HashIndex<GenericKey<96>, ItemPointer, GenericHasher<96>,
                         GenericComparator<96>, GenericEqualityChecker<96>>;
template class HashIndex<GenericKey<128>, ItemPointer, GenericHasher<128>,
                         GenericComparator<128>, GenericEqualityChecker<128>>;
template class HashIndex<GenericKey<256>, ItemPointer, GenericHasher<256>,
                         GenericComparator<256>, GenericEqualityChecker<256>>;
template class HashIndex<GenericKey<512>, ItemPointer, GenericHasher<512>,
                         GenericComparator<512>, GenericEqualityChecker<512>>;

template class HashIndex<TupleKey, ItemPointer, TupleKeyHasher,
                         TupleKeyComparator, TupleKeyEqualityChecker>;

}  // End index namespace
}  // End peloton namespace
