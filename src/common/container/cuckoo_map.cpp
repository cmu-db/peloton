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

#include "common/container/cuckoo_map.h"
#include "common/internal_types.h"
#include "common/logger.h"
#include "common/macros.h"

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

}  // namespace peloton
