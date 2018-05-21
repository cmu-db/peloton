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
    compaction_threshold_(settings::SettingsManager::GetDouble(settings::SettingId::compaction_threshold)),
    tile_group_freeing_(settings::SettingsManager::GetBool(settings::SettingId::tile_group_freeing)) {}

  virtual ~GCManager() {}

  static GCManager &GetInstance() {
    static GCManager gc_manager;
    return gc_manager;
  }

  virtual void Reset() {
    is_running_ = false;
    compaction_threshold_ = settings::SettingsManager::GetDouble(settings::SettingId::compaction_threshold);
    tile_group_freeing_ = settings::SettingsManager::GetBool(settings::SettingId::tile_group_freeing);
  }

  // Get status of whether GC thread is running or not
  bool GetStatus() { return this->is_running_; }

  virtual void StartGC(
      std::vector<std::unique_ptr<std::thread>> &UNUSED_ATTRIBUTE) {}

  virtual void StartGC() {}

  virtual void StopGC() {}

  virtual ItemPointer GetRecycledTupleSlot(storage::DataTable *table UNUSED_ATTRIBUTE) {
    return INVALID_ITEMPOINTER;
  }

  virtual void RecycleTupleSlot(const ItemPointer &location UNUSED_ATTRIBUTE) {}

  virtual void RegisterTable(oid_t table_id UNUSED_ATTRIBUTE) {}

  virtual void DeregisterTable(const oid_t &table_id UNUSED_ATTRIBUTE) {}

  virtual size_t GetTableCount() { return 0; }

  virtual void RecycleTransaction(
                      concurrency::TransactionContext *txn UNUSED_ATTRIBUTE) {}

  virtual void AddToImmutableQueue(const oid_t &tile_group_id UNUSED_ATTRIBUTE) {}

  void SetCompactionThreshold(double threshold) { compaction_threshold_ = threshold; }

  double GetCompactionThreshold() const { return compaction_threshold_; }

  void SetTileGroupFreeing(bool free) { tile_group_freeing_ = free; }

  bool GetTileGroupFreeing() const {return tile_group_freeing_; }

 protected:
  void CheckAndReclaimVarlenColumns(storage::TileGroup *tile_group,
                                    oid_t tuple_id);

 protected:

  volatile bool is_running_;
  volatile double compaction_threshold_;
  volatile bool tile_group_freeing_;
};

}  // namespace gc
}  // namespace peloton
