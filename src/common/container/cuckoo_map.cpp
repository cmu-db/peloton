//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cuckoo_map.cpp
//
// Identification: src/container/cuckoo_map.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <functional>
#include <iostream>

#include "catalog/schema.h"
#include "common/container/cuckoo_map.h"
#include "common/internal_types.h"
#include "common/item_pointer.h"
#include "common/logger.h"
#include "common/macros.h"
#include "index/compact_ints_key.h"
#include "index/generic_key.h"
#include "index/tuple_key.h"
#include "index/index_key.h"
#include "storage/tuple.h"
#include "type/value_factory.h"

namespace peloton {

namespace storage {
class TileGroup;
}

namespace stats {
class BackendStatsContext;
class IndexMetric;
}  // namespace stats

class StatementCache;

CUCKOO_MAP_TEMPLATE_ARGUMENTS
CUCKOO_MAP_TYPE::CuckooMap() {}

CUCKOO_MAP_TEMPLATE_ARGUMENTS
CUCKOO_MAP_TYPE::~CuckooMap() {}

CUCKOO_MAP_TEMPLATE_ARGUMENTS
bool CUCKOO_MAP_TYPE::Insert(const KeyType &key, ValueType value) {
  auto status = cuckoo_map.insert(key, value);
  LOG_TRACE("insert status : %d", status);
  return status;
}

CUCKOO_MAP_TEMPLATE_ARGUMENTS
bool CUCKOO_MAP_TYPE::Update(const KeyType &key, ValueType value) {
  auto status = cuckoo_map.update(key, value);
  return status;
}

CUCKOO_MAP_TEMPLATE_ARGUMENTS
bool CUCKOO_MAP_TYPE::Erase(const KeyType &key) {
  auto status = cuckoo_map.erase(key);
  LOG_TRACE("erase status : %d", status);
  return status;
}

CUCKOO_MAP_TEMPLATE_ARGUMENTS
bool CUCKOO_MAP_TYPE::Find(const KeyType &key, ValueType &value) const {
  auto status = cuckoo_map.find(key, value);
  LOG_TRACE("find status : %d", status);
  return status;
}

CUCKOO_MAP_TEMPLATE_ARGUMENTS
ValueType CUCKOO_MAP_TYPE::GetValue(const KeyType &key) const {
  return cuckoo_map.find(key);
}

CUCKOO_MAP_TEMPLATE_ARGUMENTS
bool CUCKOO_MAP_TYPE::Contains(const KeyType &key) {
  return cuckoo_map.contains(key);
}

CUCKOO_MAP_TEMPLATE_ARGUMENTS
void CUCKOO_MAP_TYPE::Clear() { cuckoo_map.clear(); }

CUCKOO_MAP_TEMPLATE_ARGUMENTS
size_t CUCKOO_MAP_TYPE::GetSize() const { return cuckoo_map.size(); }

CUCKOO_MAP_TEMPLATE_ARGUMENTS
bool CUCKOO_MAP_TYPE::IsEmpty() const { return cuckoo_map.empty(); }

CUCKOO_MAP_TEMPLATE_ARGUMENTS
CUCKOO_MAP_ITERATOR_TYPE
CUCKOO_MAP_TYPE::GetIterator() { return cuckoo_map.lock_table(); }

// Explicit template instantiation
template class CuckooMap<uint32_t, uint32_t>;

template class CuckooMap<oid_t, std::shared_ptr<storage::TileGroup>>;

// Used in Shared Pointer test and iterator test
template class CuckooMap<oid_t, std::shared_ptr<oid_t>>;

template class CuckooMap<std::thread::id,
                         std::shared_ptr<stats::BackendStatsContext>>;

template class CuckooMap<oid_t, std::shared_ptr<stats::IndexMetric>>;

// Used in SharedPointerKeyTest
template class CuckooMap<std::shared_ptr<oid_t>, std::shared_ptr<oid_t>>;

// Used in StatementCacheManager
template class CuckooMap<StatementCache *, StatementCache *>;

// Used in Cuckoo Hash Index
// Compact Ints Key
template class CuckooMap<index::CompactIntsKey<1>, ItemPointer *,
                         index::CompactIntsHasher<1>,
                         index::CompactIntsEqualityChecker<1>>;
template class CuckooMap<index::CompactIntsKey<2>, ItemPointer *,
                         index::CompactIntsHasher<2>,
                         index::CompactIntsEqualityChecker<2>>;
template class CuckooMap<index::CompactIntsKey<3>, ItemPointer *,
                         index::CompactIntsHasher<3>,
                         index::CompactIntsEqualityChecker<3>>;
template class CuckooMap<index::CompactIntsKey<4>, ItemPointer *,
                         index::CompactIntsHasher<4>,
                         index::CompactIntsEqualityChecker<4>>;

// Generic key
template class CuckooMap<index::GenericKey<4>, ItemPointer *,
                         index::GenericHasher<4>,
                         index::GenericEqualityChecker<4>>;
template class CuckooMap<index::GenericKey<8>, ItemPointer *,
                         index::GenericHasher<8>,
                         index::GenericEqualityChecker<8>>;
template class CuckooMap<index::GenericKey<16>, ItemPointer *,
                         index::GenericHasher<16>,
                         index::GenericEqualityChecker<16>>;
template class CuckooMap<index::GenericKey<64>, ItemPointer *,
                         index::GenericHasher<64>,
                         index::GenericEqualityChecker<64>>;
template class CuckooMap<index::GenericKey<256>, ItemPointer *,
                         index::GenericHasher<256>,
                         index::GenericEqualityChecker<256>>;

// Tuple key
template class CuckooMap<index::TupleKey, ItemPointer *,
                         index::TupleKeyHasher,
                         index::TupleKeyEqualityChecker>;

}  // namespace peloton
