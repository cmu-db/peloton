//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// gc_manager.h
//
// Identification: src/include/gc/gc_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <thread>
#include <vector>

#include "common/item_pointer.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/internal_types.h"
#include "settings/settings_manager.h"
#include "storage/data_table.h"

namespace peloton {

namespace concurrency {
class TransactionContext;
}

namespace storage {
class TileGroup;
}

namespace gc {

//===--------------------------------------------------------------------===//
// GC Manager
//===--------------------------------------------------------------------===//

class GCManager {
 public:
  GCManager(const GCManager &) = delete;
  GCManager &operator=(const GCManager &) = delete;
  GCManager(GCManager &&) = delete;
  GCManager &operator=(GCManager &&) = delete;

  GCManager() : is_running_(false),
    tile_group_recycling_threshold_(settings::SettingsManager::GetDouble(settings::SettingId::tile_group_recycling_threshold)),
    tile_group_freeing_(settings::SettingsManager::GetBool(settings::SettingId::tile_group_freeing)),
    tile_group_compaction_(settings::SettingsManager::GetBool(settings::SettingId::tile_group_compaction)) {}

  virtual ~GCManager() {}

  static GCManager &GetInstance() {
    static GCManager gc_manager;
    return gc_manager;
  }

  virtual void Reset() {
    is_running_ = false;
    tile_group_recycling_threshold_ = settings::SettingsManager::GetDouble(settings::SettingId::tile_group_recycling_threshold);
    tile_group_freeing_ = settings::SettingsManager::GetBool(settings::SettingId::tile_group_freeing);
    tile_group_compaction_ = settings::SettingsManager::GetBool(settings::SettingId::tile_group_compaction);
  }

  // Get status of whether GC thread is running or not
  bool GetStatus() { return is_running_; }

  virtual void StartGC(
      std::vector<std::unique_ptr<std::thread>> &UNUSED_ATTRIBUTE) {}

  virtual void StartGC() {}

  virtual void StopGC() {}

  virtual ItemPointer GetRecycledTupleSlot(storage::DataTable *table UNUSED_ATTRIBUTE) {
    return INVALID_ITEMPOINTER;
  }

  virtual void RecycleTupleSlot(const ItemPointer &location UNUSED_ATTRIBUTE) {}

  virtual void RegisterTable(const oid_t &table_id UNUSED_ATTRIBUTE) {}

  virtual void DeregisterTable(const oid_t &table_id UNUSED_ATTRIBUTE) {}

  virtual size_t GetTableCount() { return 0; }

  virtual void RecycleTransaction(
                      concurrency::TransactionContext *txn UNUSED_ATTRIBUTE) {}

  virtual void AddToImmutableQueue(const oid_t &tile_group_id UNUSED_ATTRIBUTE) {}

  virtual void SetTileGroupRecyclingThreshold(const double &threshold UNUSED_ATTRIBUTE) {}

  virtual double GetTileGroupRecyclingThreshold() const { return tile_group_recycling_threshold_; }

  virtual void SetTileGroupFreeing(const bool &free UNUSED_ATTRIBUTE) {}

  virtual bool GetTileGroupFreeing() const { return tile_group_freeing_; }

  virtual void SetTileGroupCompaction(const bool &compact UNUSED_ATTRIBUTE) {}

  virtual bool GetTileGroupCompaction() const { return tile_group_compaction_; }

 protected:
  void CheckAndReclaimVarlenColumns(storage::TileGroup *tile_group,
                                    oid_t tuple_id);

 protected:

  volatile bool is_running_;
  volatile double tile_group_recycling_threshold_;
  volatile bool tile_group_freeing_;
  volatile bool tile_group_compaction_;
};

}  // namespace gc
}  // namespace peloton
