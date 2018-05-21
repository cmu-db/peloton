//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tile_group_compactor.h
//
// Identification: src/include/gc/transaction_level_gc_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <map>
#include <thread>
#include <unordered_map>
#include <vector>

#include "brain/query_logger.h"
#include "catalog/manager.h"
#include "catalog/catalog.h"
#include "common/container_tuple.h"
#include "common/logger.h"
#include "common/thread_pool.h"
#include "common/internal_types.h"
#include "concurrency/transaction_context.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/executor_context.h"
#include "index/index.h"
#include "settings/settings_manager.h"
#include "storage/database.h"
#include "storage/storage_manager.h"
#include "storage/tile_group.h"
#include "storage/tuple.h"
#include "threadpool/mono_queue_pool.h"

namespace peloton {

namespace gc {

class TileGroupCompactor {

 public:
  
  /**
   * @brief Repeatedly tries to move all of the tuples out of a TileGroup
   * in a transactional manner.
   * 
   * This function is intended to be added to the MonoQueuePool as a task.
   * 
   * @param[in] tile_group_id Global oid of the TileGroup to be compacted.
   * TileGroup should be marked as immutable first otherwise tuples may
   * be reinserted into the same TileGroup.
   */
  static void CompactTileGroup(const oid_t &tile_group_id);
  
  /**
   * @brief Creates a transaction and performs Updates on each visible
   * tuple with the same tuple contents.
   * 
   * The net effect is that all visible tuples are reinserted into the
   * table in other TileGroups.
   * Intended to be used by CompactTileGroup(), but can also be modified
   * to handle online schema changes.
   * 
   * @param[in] table Pointer to the table for this request
   * @param[in] tile_group Smart pointer to the TileGroup for this request.
   * TileGroup should be marked as immutable first otherwise tuples may
   * be reinserted into the same TileGroup.
   * @return True if transaction succeeds, false otherwise
   */
  static bool MoveTuplesOutOfTileGroup(storage::DataTable *table,
                                std::shared_ptr<storage::TileGroup> tile_group);
};
}
}  // namespace peloton
