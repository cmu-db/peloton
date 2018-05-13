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
#include "statistics/database_metric.h"
#include "statistics/index_metric.h"
#include "statistics/stats_event_type.h"
#include "statistics/table_metric.h"
#include "statistics/test_metric.h"
#include "statistics/tuple_access_metric.h"

namespace peloton {
namespace stats {
using CollectorsMap =
tbb::concurrent_unordered_map<std::thread::id, ThreadLevelStatsCollector,
                              std::hash<std::thread::id>>;

CollectorsMap ThreadLevelStatsCollector::collector_map_ = CollectorsMap();

ThreadLevelStatsCollector::ThreadLevelStatsCollector() {
  // TODO(tianyu): Write stats to register here
  auto
      stats_mode = static_cast<StatsModeType>(settings::SettingsManager::GetInt(
      settings::SettingId::stats_mode));
  if (stats_mode == StatsModeType::ENABLE) {
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
    RegisterMetric<TupleAccessMetric>({StatsEventType::TXN_BEGIN,
                                       StatsEventType::TXN_ABORT,
                                       StatsEventType::TXN_COMMIT,
                                       StatsEventType::TUPLE_READ});
  } else if (stats_mode == StatsModeType::TEST)
    RegisterMetric<TestMetric>({StatsEventType::TEST});
}

ThreadLevelStatsCollector::~ThreadLevelStatsCollector() {
  metrics_.clear();
  metric_dispatch_.clear();
}

std::vector<std::shared_ptr<AbstractRawData>> ThreadLevelStatsCollector::GetDataToAggregate() {
  std::vector<std::shared_ptr<AbstractRawData>> result;
  for (auto &metric : metrics_) result.push_back(metric->Swap());
  return result;
}
}  // namespace stats
}  // namespace peloton
