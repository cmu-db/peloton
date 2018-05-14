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

  // This function is what gets put in the MonoQueuePool as a task
  // It repeatedly tries to compact a tile group, until it succeeds
  // or max_attempts is exceeded.
  static void CompactTileGroup(const oid_t &tile_group_id);

  // Worker function used by CompactTileGroup() to move tuples to new tile group
  static bool MoveTuplesOutOfTileGroup(storage::DataTable *table,
                                std::shared_ptr<storage::TileGroup> tile_group);
};
}
}  // namespace peloton
