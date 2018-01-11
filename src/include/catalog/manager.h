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
#include "common/internal_types.h"
#include "common/container/lock_free_array.h"

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
  // TILE GROUP ALLOCATION
  //===--------------------------------------------------------------------===//

  oid_t GetNextTileId() { return ++tile_oid_; }

  oid_t GetNextTileGroupId() { return ++tile_group_oid_; }

  oid_t GetCurrentTileGroupId() { return tile_group_oid_; }

  void SetNextTileGroupId(oid_t next_oid) { tile_group_oid_ = next_oid; }

  void AddTileGroup(const oid_t oid,
                    std::shared_ptr<storage::TileGroup> location);

  void DropTileGroup(const oid_t oid);

  std::shared_ptr<storage::TileGroup> GetTileGroup(const oid_t oid);

  void ClearTileGroup(void);


  //===--------------------------------------------------------------------===//
  // INDIRECTION ARRAY ALLOCATION
  //===--------------------------------------------------------------------===//

  oid_t GetNextIndirectionArrayId() { return ++indirection_array_oid_; }

  oid_t GetCurrentIndirectionArrayId() { return indirection_array_oid_; }

  void AddIndirectionArray(const oid_t oid,
                           std::shared_ptr<storage::IndirectionArray> location);

  void DropIndirectionArray(const oid_t oid);

  void ClearIndirectionArray(void);

  Manager(Manager const &) = delete;

 private:
  //===--------------------------------------------------------------------===//
  // Data member for tile allocation
  //===--------------------------------------------------------------------===//

  std::atomic<oid_t> tile_oid_ = ATOMIC_VAR_INIT(START_OID);

  //===--------------------------------------------------------------------===//
  // Data members for tile group allocation
  //===--------------------------------------------------------------------===//
  std::atomic<oid_t> tile_group_oid_ = ATOMIC_VAR_INIT(START_OID);

  LockFreeArray<std::shared_ptr<storage::TileGroup>> tile_group_locator_;

  static std::shared_ptr<storage::TileGroup> empty_tile_group_;

  //===--------------------------------------------------------------------===//
  // Data members for indirection array allocation
  //===--------------------------------------------------------------------===//
  std::atomic<oid_t> indirection_array_oid_ = ATOMIC_VAR_INIT(START_OID);

  LockFreeArray<std::shared_ptr<storage::IndirectionArray>> indirection_array_locator_;

  static std::shared_ptr<storage::IndirectionArray> empty_indirection_array_;
};

}  // namespace catalog
}  // namespace peloton
