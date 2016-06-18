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

#include "backend/index/rb_hash_index.h"
#include "backend/index/index_key.h"
#include "backend/common/logger.h"
#include "backend/storage/tuple.h"

namespace peloton {
namespace index {

template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
RBHashIndex<KeyType, ValueType, KeyHasher, KeyComparator,
          KeyEqualityChecker>::RBHashIndex(IndexMetadata *metadata, UNUSED_ATTRIBUTE const size_t &preallocate_size)
    : Index(metadata),
      container(KeyHasher(metadata), KeyEqualityChecker(metadata), preallocate_size),
      hasher(metadata),
      equals(metadata),
      comparator(metadata) { }
      

template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
RBHashIndex<KeyType, ValueType, KeyHasher, KeyComparator,
          KeyEqualityChecker>::~RBHashIndex() {
  // we should not rely on shared_ptr to reclaim memory.
  // this is because the underlying index can split or merge leaf nodes,
  // which invokes data data copy and deletes.
  // as the underlying index is unaware of shared_ptr,
  // memory allocated should be managed carefully by programmers.
  auto lt = container.lock_table();
  for (const auto &entry_vector : lt) {
    for (auto entry : entry_vector.second) {
      delete entry;
      entry = nullptr;
    }
  }
}

// void insert_helper(std::vector<ItemPointer*> &existing_vector, void *new_location) {
//   existing_vector.push_back((ItemPointer*)new_location);
// }

template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
bool RBHashIndex<KeyType, ValueType, KeyHasher, KeyComparator,
               KeyEqualityChecker>::InsertEntry(const storage::Tuple *key,
                                                const ItemPointer &location,
                                                RBItemPointer **result) {
  KeyType index_key;

  index_key.SetFromKey(key);

  *result = new RBItemPointer(location, MAX_CID);
  std::vector<ValueType> val;
  val.push_back(*result);
  // if there's no key in the hash map, then insert a vector containing location.
  // otherwise, directly insert location into the vector that already exists in the hash map.
  container.upsert(index_key, 
    [](std::vector<RBItemPointer *> &existing_vector, void *new_location){
        existing_vector.push_back((RBItemPointer *)new_location);
    }, 
    (void *)*result, val);

  return true;
}

template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
bool RBHashIndex<KeyType, ValueType, KeyHasher, KeyComparator,
               KeyEqualityChecker>::DeleteEntry(const storage::Tuple *key,
                                                const RBItemPointer &location) {
  KeyType index_key;

  index_key.SetFromKey(key);

  // TODO: add retry logic
  container.update_fn(index_key, 
    [](std::vector<RBItemPointer*> &existing_vector, void *old_location) {
      existing_vector.erase(std::remove_if(existing_vector.begin(), existing_vector.end(),
                               RBItemPointerEqualityCheckerWithTS(*((RBItemPointer*)old_location))),
      existing_vector.end());
    },
    (void *)&location);

  return true;
}

template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
bool RBHashIndex<KeyType, ValueType, KeyHasher, KeyComparator,
               KeyEqualityChecker>::DeleteEntry(const storage::Tuple *key,
                                                const ItemPointer &location) {
  KeyType index_key;
  index_key.SetFromKey(key);

  // TODO: add retry logic
  container.update_fn(index_key, 
    [](std::vector<RBItemPointer*> &existing_vector, void *old_location) {
      existing_vector.erase(std::remove_if(existing_vector.begin(), existing_vector.end(),
                               RBItemPointerEqualityChecker(*((RBItemPointer*)old_location))),
      existing_vector.end());
    },
    (void *)&location);

  return true;
}

template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
bool RBHashIndex<KeyType, ValueType, KeyHasher, KeyComparator,
                KeyEqualityChecker>::CondInsertEntry(
    const storage::Tuple *key, const ItemPointer &location,
    std::function<bool(const void *)> predicate UNUSED_ATTRIBUTE,
    RBItemPointer **rb_itempointer_ptr) {

  KeyType index_key;
  index_key.SetFromKey(key);

  RBItemPointer *new_location = new RBItemPointer(location, MAX_CID);
  std::vector<ValueType> val;
  val.push_back(new_location);

  *rb_itempointer_ptr = new_location;

  container.upsert(index_key, 
    [](std::vector<RBItemPointer *> &existing_vector, void *location, std::function<bool(const void *)> predicate UNUSED_ATTRIBUTE, void **arg_ptr UNUSED_ATTRIBUTE) {
      existing_vector.push_back((RBItemPointer*)location);
      return;
    }, 
    (void*)new_location, predicate, (void**)rb_itempointer_ptr, val);

  if (*rb_itempointer_ptr == new_location) {
    return true;
  } else {
    LOG_TRACE("predicate fails, abort transaction");
    delete new_location;
    new_location = nullptr;
    return false;
  }
}


template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
void RBHashIndex<KeyType, ValueType, KeyHasher, KeyComparator, 
             KeyEqualityChecker>::Scan(const std::vector<Value> &values,
                                       const std::vector<oid_t> &key_column_ids,
                                       const std::vector<ExpressionType> &expr_types,
                                       UNUSED_ATTRIBUTE const ScanDirectionType &scan_direction,
                                       std::vector<RBItemPointer> &result) {
  KeyType index_key;
  std::unique_ptr<storage::Tuple> start_key;
  start_key.reset(new storage::Tuple(metadata->GetKeySchema(), true));

  bool all_constraints_are_equal = ConstructLowerBoundTuple(
          start_key.get(), values, key_column_ids, expr_types);
  if (all_constraints_are_equal == false) {
    LOG_ERROR("not all constraints are equal!");
    assert(false);
  }

  index_key.SetFromKey(start_key.get());
  
  std::vector<RBItemPointer*> tmp_result;
  container.find(index_key, tmp_result);
  for (auto entry : tmp_result) {
    result.push_back(RBItemPointer(*entry));
  }
}


template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
void RBHashIndex<KeyType, ValueType, KeyHasher, KeyComparator, KeyEqualityChecker>::ScanAllKeys(
    std::vector<RBItemPointer> &result) {
  {
    auto lt = container.lock_table();
    for (const auto &itr : lt) {
      for (const auto entry : itr.second) {
        result.push_back(RBItemPointer(*entry));
      }
    }
  }
}

/**
 * @brief Return all locations related to this key.
 */
template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
void RBHashIndex<KeyType, ValueType, KeyHasher, KeyComparator, KeyEqualityChecker>::ScanKey(
    const storage::Tuple *key, std::vector<RBItemPointer> &result) {
  KeyType index_key;
  index_key.SetFromKey(key);

  std::vector<RBItemPointer*> values;

  bool ret = container.find(index_key, values);

  if (ret == true) {
    for (auto entry : values) {
      result.push_back(*entry);
    }
  } else {
    assert(result.size() == 0);
  }
}


template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
void RBHashIndex<KeyType, ValueType, KeyHasher, KeyComparator, 
             KeyEqualityChecker>::Scan(const std::vector<Value> &values,
                                       const std::vector<oid_t> &key_column_ids,
                                       const std::vector<ExpressionType> &expr_types,
                                       UNUSED_ATTRIBUTE const ScanDirectionType &scan_direction,
                                       std::vector<RBItemPointer *> &result) {

  KeyType index_key;
  std::unique_ptr<storage::Tuple> start_key;
  start_key.reset(new storage::Tuple(metadata->GetKeySchema(), true));

  bool all_constraints_are_equal = ConstructLowerBoundTuple(
          start_key.get(), values, key_column_ids, expr_types);
  if (all_constraints_are_equal == false) {
    LOG_ERROR("not all constraints are equal!");
    assert(false);
  }

  index_key.SetFromKey(start_key.get());
  
  container.find(index_key, result);
}


template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
void RBHashIndex<KeyType, ValueType, KeyHasher, KeyComparator, 
             KeyEqualityChecker>::ScanAllKeys(std::vector<RBItemPointer *> &result) {
  {
    auto lt = container.lock_table();
    for (const auto &itr : lt) {
      result.insert(result.end(), itr.second.begin(), itr.second.end());
    }
  }
}

/**
 * @brief Return all locations related to this key.
 */
template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
void RBHashIndex<KeyType, ValueType, KeyHasher, KeyComparator, KeyEqualityChecker>::ScanKey(
    const storage::Tuple *key, std::vector<RBItemPointer *> &result) {
  KeyType index_key;
  index_key.SetFromKey(key);

  container.find(index_key, result);
}


template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
std::string RBHashIndex<KeyType, ValueType, KeyHasher, KeyComparator,
                      KeyEqualityChecker>::GetTypeName() const {
  return "RBHash";
}

// Original method
template <typename KeyType, typename ValueType, class KeyHasher, class KeyComparator,
          class KeyEqualityChecker>
bool RBHashIndex<KeyType, ValueType, KeyHasher, KeyComparator,
                 KeyEqualityChecker>::InsertEntry(UNUSED_ATTRIBUTE const
                                                  storage::Tuple *key,
                                                  UNUSED_ATTRIBUTE const
                                                  ItemPointer &location) {
  // Add your implementation here
  return false;
}

template <typename KeyType, typename ValueType, class KeyHasher, class KeyComparator,
          class KeyEqualityChecker>
bool RBHashIndex<KeyType, ValueType, KeyHasher, KeyComparator,
                 KeyEqualityChecker>::CondInsertEntry(
    UNUSED_ATTRIBUTE const storage::Tuple *key,
    UNUSED_ATTRIBUTE const ItemPointer &location,
    UNUSED_ATTRIBUTE std::function<bool(const void *)> predicate,
    UNUSED_ATTRIBUTE ItemPointer **itemptr_ptr ) {
  return false;
}

template <typename KeyType, typename ValueType, class KeyHasher, class KeyComparator,
          class KeyEqualityChecker>
void RBHashIndex<KeyType, ValueType, KeyHasher, KeyComparator, KeyEqualityChecker>::Scan(
    UNUSED_ATTRIBUTE const std::vector<Value> &values,
    UNUSED_ATTRIBUTE const std::vector<oid_t> &key_column_ids,
    UNUSED_ATTRIBUTE const std::vector<ExpressionType> &expr_types,
    UNUSED_ATTRIBUTE const ScanDirectionType &scan_direction,
    UNUSED_ATTRIBUTE std::vector<ItemPointer> &result) {}

template <typename KeyType, typename ValueType, class KeyHasher, class KeyComparator,
          class KeyEqualityChecker>
void RBHashIndex<KeyType, ValueType, KeyHasher, KeyComparator, KeyEqualityChecker>::ScanAllKeys(
    UNUSED_ATTRIBUTE std::vector<ItemPointer> &result) {}

template <typename KeyType, typename ValueType, class KeyHasher, class KeyComparator,
          class KeyEqualityChecker>
void
RBHashIndex<KeyType, ValueType, KeyHasher, KeyComparator, KeyEqualityChecker>::ScanKey(
    UNUSED_ATTRIBUTE const storage::Tuple *key,
    UNUSED_ATTRIBUTE std::vector<ItemPointer> &result) {}

template <typename KeyType, typename ValueType, class KeyHasher, class KeyComparator,
          class KeyEqualityChecker>
void RBHashIndex<KeyType, ValueType, KeyHasher, KeyComparator, KeyEqualityChecker>::Scan(
    UNUSED_ATTRIBUTE const std::vector<Value> &values,
    UNUSED_ATTRIBUTE const std::vector<oid_t> &key_column_ids,
    UNUSED_ATTRIBUTE const std::vector<ExpressionType> &expr_types,
    UNUSED_ATTRIBUTE const ScanDirectionType &scan_direction,
    UNUSED_ATTRIBUTE std::vector<ItemPointer *> &result) {}

template <typename KeyType, typename ValueType, class KeyHasher, class KeyComparator,
          class KeyEqualityChecker>
void
RBHashIndex<KeyType, ValueType, KeyHasher, KeyComparator, KeyEqualityChecker>::ScanAllKeys(
    UNUSED_ATTRIBUTE std::vector<ItemPointer *> &result) {}

template <typename KeyType, typename ValueType, class KeyHasher, class KeyComparator,
          class KeyEqualityChecker>
void
RBHashIndex<KeyType, ValueType, KeyHasher, KeyComparator, KeyEqualityChecker>::ScanKey(
    UNUSED_ATTRIBUTE const storage::Tuple *key,
    UNUSED_ATTRIBUTE std::vector<ItemPointer *> &result) {}

// Explicit template instantiation
template class RBHashIndex<IntsKey<1>, RBItemPointer *, IntsHasher<1>,
                         IntsComparator<1>, IntsEqualityChecker<1>>;
template class RBHashIndex<IntsKey<2>, RBItemPointer *, IntsHasher<2>,
                         IntsComparator<2>, IntsEqualityChecker<2>>;
template class RBHashIndex<IntsKey<3>, RBItemPointer *, IntsHasher<3>,
                         IntsComparator<3>, IntsEqualityChecker<3>>;
template class RBHashIndex<IntsKey<4>, RBItemPointer *, IntsHasher<4>,
                         IntsComparator<4>, IntsEqualityChecker<4>>;

template class RBHashIndex<GenericKey<4>, RBItemPointer *, GenericHasher<4>,
                         GenericComparator<4>, GenericEqualityChecker<4>>;
template class RBHashIndex<GenericKey<8>, RBItemPointer *, GenericHasher<8>,
                         GenericComparator<8>, GenericEqualityChecker<8>>;
template class RBHashIndex<GenericKey<12>, RBItemPointer *, GenericHasher<12>,
                         GenericComparator<12>, GenericEqualityChecker<12>>;
template class RBHashIndex<GenericKey<16>, RBItemPointer *, GenericHasher<16>,
                         GenericComparator<16>, GenericEqualityChecker<16>>;
template class RBHashIndex<GenericKey<24>, RBItemPointer *, GenericHasher<24>,
                         GenericComparator<24>, GenericEqualityChecker<24>>;
template class RBHashIndex<GenericKey<32>, RBItemPointer *, GenericHasher<32>,
                         GenericComparator<32>, GenericEqualityChecker<32>>;
template class RBHashIndex<GenericKey<48>, RBItemPointer *, GenericHasher<48>,
                         GenericComparator<48>, GenericEqualityChecker<48>>;
template class RBHashIndex<GenericKey<64>, RBItemPointer *, GenericHasher<64>,
                         GenericComparator<64>, GenericEqualityChecker<64>>;
template class RBHashIndex<GenericKey<96>, RBItemPointer *, GenericHasher<96>,
                         GenericComparator<96>, GenericEqualityChecker<96>>;
template class RBHashIndex<GenericKey<128>, RBItemPointer *, GenericHasher<128>,
                         GenericComparator<128>, GenericEqualityChecker<128>>;
template class RBHashIndex<GenericKey<256>, RBItemPointer *, GenericHasher<256>,
                         GenericComparator<256>, GenericEqualityChecker<256>>;
template class RBHashIndex<GenericKey<512>, RBItemPointer *, GenericHasher<512>,
                         GenericComparator<512>, GenericEqualityChecker<512>>;

template class RBHashIndex<TupleKey, RBItemPointer *, TupleKeyHasher,
                         TupleKeyComparator, TupleKeyEqualityChecker>;

}  // End index namespace
}  // End peloton namespace