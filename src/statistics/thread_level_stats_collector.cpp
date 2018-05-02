//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// thread_level_stats_collector.cpp
//
// Identification: src/statistics/thread_level_stats_collector.cpp
//
// Copyright (c) 2017-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "statistics/thread_level_stats_collector.h"

namespace peloton {
namespace stats {
ThreadLevelStatsCollector::ThreadLevelStatsCollector() {
  // TODO(tianyu): Write stats to register here
  if (static_cast<StatsModeType>(settings::SettingsManager::GetInt(
          settings::SettingId::stats_mode)) != StatsModeType::ENABLE) {
    // TODO(tianyi): Have more fine grained control for these metrics
    RegisterMetric<TableMetric>(
        {stats_event_type::TUPLE_READ, stats_event_type::TUPLE_UPDATE,
         stats_event_type::TUPLE_INSERT, stats_event_type::TUPLE_DELETE,
         stats_event_type::TABLE_MEMORY_ALLOC,
         stats_event_type::TABLE_MEMORY_FREE});
    RegisterMetric<IndexMetric>(
        {stats_event_type::INDEX_READ, stats_event_type::INDEX_UPDATE,
         stats_event_type::INDEX_INSERT, stats_event_type::INDEX_DELETE});

    RegisterMetric<DatabaseMetric>({stats_event_type::TXN_BEGIN,
                                    stats_event_type::TXN_COMMIT,
                                    stats_event_type::TXN_ABORT});
  }
}
}  // namespace stats
}  // namespace peloton