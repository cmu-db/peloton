//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// manager.h
//
// Identification: src/include/catalog/manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <utility>
#include <mutex>
#include <vector>
#include <unordered_map>
#include <memory>
#include "common/internal_types.h"


#include "common/internal_types.h"
#include "common/macros.h"
#include "tbb/concurrent_unordered_map.h"
namespace peloton {

namespace storage {
class TileGroup;
class IndirectionArray;
}

namespace catalog {

//===--------------------------------------------------------------------===//
// Manager
//===--------------------------------------------------------------------===//

class Manager {
 public:
  Manager() {}

  // Singleton
  static Manager &GetInstance();

  //===--------------------------------------------------------------------===//
  // INDIRECTION ARRAY ALLOCATION
  //===--------------------------------------------------------------------===//

  oid_t GetNextIndirectionArrayId() { return ++indirection_array_oid_; }

  oid_t GetCurrentIndirectionArrayId() const { return indirection_array_oid_; }

  void AddIndirectionArray(const oid_t &oid,
                           std::shared_ptr<storage::IndirectionArray> location);

  void DropIndirectionArray(const oid_t &oid);

  void ClearIndirectionArrays(void);

  Manager(Manager const &) = delete;

 private:

  //===--------------------------------------------------------------------===//
  // Data members for indirection array allocation
  //===--------------------------------------------------------------------===//
  std::atomic<oid_t> indirection_array_oid_ = ATOMIC_VAR_INIT(START_OID);

  tbb::concurrent_unordered_map<oid_t,
                                std::shared_ptr<storage::IndirectionArray>>
      indirection_array_locator_;
};

}  // namespace catalog
}  // namespace peloton
