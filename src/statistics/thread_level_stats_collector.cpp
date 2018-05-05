//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// thread_level_stats_collector.cpp
//
// Identification: src/include/statistics/thread_level_stats_collector.cpp
//
// Copyright (c) 2017-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "statistics/thread_level_stats_collector.h"

namespace peloton {
namespace stats {
using CollectorsMap =
    tbb::concurrent_unordered_map<std::thread::id, ThreadLevelStatsCollector,
                                  std::hash<std::thread::id>>;

CollectorsMap ThreadLevelStatsCollector::collector_map_ = CollectorsMap();

ThreadLevelStatsCollector::ThreadLevelStatsCollector() {
  // TODO(tianyu): Write stats to register here
  if (static_cast<StatsModeType>(settings::SettingsManager::GetInt(
          settings::SettingId::stats_mode)) == StatsModeType::ENABLE) {
    // TODO(tianyi): Have more fine grained control for these metrics
    RegisterMetric<TableMetric>(
        {StatsEventType::TUPLE_READ, StatsEventType::TUPLE_UPDATE,
         StatsEventType::TUPLE_INSERT, StatsEventType::TUPLE_DELETE,
         StatsEventType::TABLE_MEMORY_ALLOC,
         StatsEventType::TABLE_MEMORY_FREE});
    RegisterMetric<IndexMetric>(
        {StatsEventType::INDEX_READ, StatsEventType::INDEX_UPDATE,
         StatsEventType::INDEX_INSERT, StatsEventType::INDEX_DELETE});

    RegisterMetric<DatabaseMetric>({StatsEventType::TXN_BEGIN,
                                    StatsEventType::TXN_COMMIT,
                                    StatsEventType::TXN_ABORT});
  }
}
}  // namespace stats
}  // namespace peloton
