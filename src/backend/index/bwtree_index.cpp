//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// btree_index.cpp
//
// Identification: src/backend/index/btree_index.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/common/logger.h"
#include "backend/index/bwtree_index.h"
#include "backend/index/index_key.h"
#include "backend/storage/tuple.h"

namespace peloton {
namespace index {

template <typename KeyType>
const BoundedKey<KeyType> BoundedKey<KeyType>::NEG_INF_KEY{IS_NEG_INF};

template <typename KeyType>
const BoundedKey<KeyType> BoundedKey<KeyType>::POS_INF_KEY{IS_POS_INF};

template <typename KeyType>
BoundedKey<KeyType>::BoundedKey()
    : key_type(IS_NEG_INF) {}

template <typename KeyType>
BoundedKey<KeyType>::BoundedKey(char key_type)
    : key_type(key_type) {}

template <typename KeyType>
BoundedKey<KeyType>::BoundedKey(KeyType key)
    : key_type(IS_REGULAR), key(key) {}

template <typename KeyType, class KeyComparator>
BoundedKeyComparator<KeyType, KeyComparator>::BoundedKeyComparator(
    KeyComparator m_key_less)
    : m_key_less(m_key_less) {}

template <typename KeyType, class KeyComparator>
bool BoundedKeyComparator<KeyType, KeyComparator>::operator()(
    const BoundedKey<KeyType> &l, const BoundedKey<KeyType> &r) const {
  const char &REGULAR = IS_REGULAR;
  const char &NEG_INF = IS_NEG_INF;
  const char &POS_INF = IS_POS_INF;

  if (l.key_type == REGULAR && r.key_type == REGULAR) {
    return m_key_less(l.key, r.key);
  } else if (l.key_type == REGULAR) {
    if (r.key_type == POS_INF) {
      return true;
    } else {
      // Is neg inf
      return false;
    }
  } else if (l.key_type == NEG_INF) {
    if (r.key_type == NEG_INF) {
      return false;
    } else {
      // Either regular or inf, either way it is greater
      return true;
    }
  } else {
    // Can never be less because left is POS_INF
    return false;
  }
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::BWTreeIndex(
    IndexMetadata *metadata)
    : Index(metadata),
      equals(metadata),
      comparator(metadata),
      container(BoundedKeyComparator<KeyType, KeyComparator>(comparator),
                metadata->unique_keys) {}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
BWTreeIndex<KeyType, ValueType, KeyComparator,
            KeyEqualityChecker>::~BWTreeIndex() {}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
bool BWTreeIndex<KeyType, ValueType, KeyComparator,
                 KeyEqualityChecker>::InsertEntry(const storage::Tuple *key,
                                                  const ItemPointer location) {
  KeyType index_key;
  bool status = false;
  index_key.SetFromKey(key);
  {
    // index_lock.WriteLock();

    // Insert the key, val pair
    status = container.insert(index_key, location);

    // index_lock.Unlock();
  }
  return status;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
bool BWTreeIndex<KeyType, ValueType, KeyComparator,
                 KeyEqualityChecker>::DeleteEntry(const storage::Tuple *key,
                                                  const ItemPointer location) {
  KeyType index_key;
  index_key.SetFromKey(key);
  bool status = false;
  {
    // index_lock.WriteLock();

    // Erase the key, val pair
    status = container.erase(index_key, location);

    // index_lock.Unlock();
  }

  return status;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
std::vector<ItemPointer>
BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Scan(
    const std::vector<Value> &values, const std::vector<oid_t> &key_column_ids,
    const std::vector<ExpressionType> &expr_types,
    const ScanDirectionType &scan_direction) {
  std::vector<ItemPointer> result;

  std::vector<std::pair<BoundedKey<KeyType>, std::vector<ValueType>>>
      key_value_result;
  container.getAllKeyValues(key_value_result);

  for (auto it = key_value_result.begin(); it != key_value_result.end(); it++) {
    auto t = it->first.key.GetTupleForComparison(metadata->GetKeySchema());

    if (Compare(t, key_column_ids, expr_types, values) == true) {
      result.insert(result.end(), it->second.begin(), it->second.end());
    }
  }

  switch (scan_direction) {
    case SCAN_DIRECTION_TYPE_FORWARD:
      break;
    case SCAN_DIRECTION_TYPE_BACKWARD:
      std::reverse(result.begin(), result.end());
      break;
    case SCAN_DIRECTION_TYPE_INVALID:
    default:
      throw Exception("Invalid scan direction \n");
      break;
  }

  return result;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
std::vector<ItemPointer> BWTreeIndex<KeyType, ValueType, KeyComparator,
                                     KeyEqualityChecker>::ScanAllKeys() {
  std::vector<ItemPointer> result;

  container.getAllValues(result);

  return result;
}

/**
 * @brief Return all locations related to this key.
 */
template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
std::vector<ItemPointer>
BWTreeIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ScanKey(
    const storage::Tuple *key) {
  KeyType index_key;
  index_key.SetFromKey(key);
  std::vector<ItemPointer> result = container.find(index_key);
  return result;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
std::string BWTreeIndex<KeyType, ValueType, KeyComparator,
                        KeyEqualityChecker>::GetTypeName() const {
  return "BWTree";
}

// Explicit template instantiation
template class BWTreeIndex<IntsKey<1>, ItemPointer, IntsComparator<1>,
                           IntsEqualityChecker<1>>;
template class BWTreeIndex<IntsKey<2>, ItemPointer, IntsComparator<2>,
                           IntsEqualityChecker<2>>;
template class BWTreeIndex<IntsKey<3>, ItemPointer, IntsComparator<3>,
                           IntsEqualityChecker<3>>;
template class BWTreeIndex<IntsKey<4>, ItemPointer, IntsComparator<4>,
                           IntsEqualityChecker<4>>;

template class BWTreeIndex<GenericKey<4>, ItemPointer, GenericComparator<4>,
                           GenericEqualityChecker<4>>;
template class BWTreeIndex<GenericKey<8>, ItemPointer, GenericComparator<8>,
                           GenericEqualityChecker<8>>;
template class BWTreeIndex<GenericKey<12>, ItemPointer, GenericComparator<12>,
                           GenericEqualityChecker<12>>;
template class BWTreeIndex<GenericKey<16>, ItemPointer, GenericComparator<16>,
                           GenericEqualityChecker<16>>;
template class BWTreeIndex<GenericKey<24>, ItemPointer, GenericComparator<24>,
                           GenericEqualityChecker<24>>;
template class BWTreeIndex<GenericKey<32>, ItemPointer, GenericComparator<32>,
                           GenericEqualityChecker<32>>;
template class BWTreeIndex<GenericKey<48>, ItemPointer, GenericComparator<48>,
                           GenericEqualityChecker<48>>;
template class BWTreeIndex<GenericKey<64>, ItemPointer, GenericComparator<64>,
                           GenericEqualityChecker<64>>;
template class BWTreeIndex<GenericKey<96>, ItemPointer, GenericComparator<96>,
                           GenericEqualityChecker<96>>;
template class BWTreeIndex<GenericKey<128>, ItemPointer, GenericComparator<128>,
                           GenericEqualityChecker<128>>;
template class BWTreeIndex<GenericKey<256>, ItemPointer, GenericComparator<256>,
                           GenericEqualityChecker<256>>;
template class BWTreeIndex<GenericKey<512>, ItemPointer, GenericComparator<512>,
                           GenericEqualityChecker<512>>;

template class BWTreeIndex<TupleKey, ItemPointer, TupleKeyComparator,
                           TupleKeyEqualityChecker>;

}  // End index namespace
}  // End peloton namespace
