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

#include "common/macros.h"
#include "common/types.h"
#include "container/cuckoo_map.h"
#include "container/lock_free_array.h"

namespace peloton {

namespace storage {
class TileGroup;
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
  // OBJECT MAP
  //===--------------------------------------------------------------------===//

  oid_t GetNextOid() { return ++oid_; }

  oid_t GetCurrentOid() { return oid_; }

  void SetNextOid(oid_t next_oid) { oid_ = next_oid; }

  void AddTileGroup(const oid_t oid,
                    std::shared_ptr<storage::TileGroup> location);

  void DropTileGroup(const oid_t oid);

  std::shared_ptr<storage::TileGroup> GetTileGroup(const oid_t oid);

  void ClearTileGroup(void);

  Manager(Manager const &) = delete;

 private:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  std::atomic<oid_t> oid_ = ATOMIC_VAR_INIT(START_OID);

  LockFreeArray<std::shared_ptr<storage::TileGroup>> locator_;

  static std::shared_ptr<storage::TileGroup> empty_location_;
};

}  // End catalog namespace
}  // End peloton namespace
