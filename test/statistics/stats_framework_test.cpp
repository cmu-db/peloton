//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// stats_framework_test.cpp
//
// Identification: test/stats/stats_framework_test.cpp
//
// Copyright (c) 2016-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"
#include "settings/settings_manager.h"
#include "statistics/stats_aggregator.h"
#include "statistics/testing_stats_util.h"

namespace peloton {
namespace test {

/**
 * @brief Test the overall correctness of the stats framework
 */
class StatsFrameworkTests : public PelotonTest {};

/**
 * @brief Single threaded test with few collection
 */
TEST_F(StatsFrameworkTests, BasicTest) {
  settings::SettingsManager::SetInt(settings::SettingId::stats_mode,
                                    static_cast<int>(StatsModeType::TEST));
  stats::ThreadLevelStatsCollector::GetCollectorForThread().CollectTestNum(1);
  stats::ThreadLevelStatsCollector::GetCollectorForThread().CollectTestNum(2);

  EXPECT_EQ(TestingStatsUtil::AggregateCounts(), 3);
};

/**
 * @brief Single threaded test with a bulk collections
 */
TEST_F(StatsFrameworkTests, SingleThreadBulkTest) {
  settings::SettingsManager::SetInt(settings::SettingId::stats_mode,
                                    static_cast<int>(StatsModeType::TEST));
  // Number of collection done in this test
  const size_t trial = 10000;
  // Aggregation is done once every aggr_step times of trial
  const size_t aggr_step = 20;
  int actual_sum = 0;
  int aggreg_sum = 0;
  for (size_t i = 0; i < trial; i++) {
    int num = rand();
    stats::ThreadLevelStatsCollector::GetCollectorForThread().CollectTestNum(num);
    actual_sum += num;

    if (!(i % aggr_step)) {
      aggreg_sum += TestingStatsUtil::AggregateCounts();
      ASSERT_EQ(actual_sum, aggreg_sum);
    }
  }

  aggreg_sum += TestingStatsUtil::AggregateCounts();
  ASSERT_EQ(actual_sum, aggreg_sum);
}

/**
 * @brief Multi threaded test running multiple collectors
 */
TEST_F(StatsFrameworkTests, MultiThreadTest) {
  settings::SettingsManager::SetInt(settings::SettingId::stats_mode,
                                    static_cast<int>(StatsModeType::TEST));
  // Number of collector thread
  const size_t num_of_collector = 10;

  // collecting interval in us
  const size_t collect_interval = 1000;

  // aggregation interval in us
  const size_t aggr_interval = 1000000;

  // Number of collection done by each collector
  const size_t collect_tials = 5000;

  // Actual sum of collectors
  std::atomic<int> actual_sum(0);

  // Aggregated sum of aggregator
  int aggreg_sum = 0;

  // Finish flag
  std::atomic<bool> finish(false);

  // start the aggregator
  std::thread aggregator([&]{
    while (!finish) {
      usleep(aggr_interval);
      aggreg_sum += TestingStatsUtil::AggregateCounts();
    }
  });

  // Start the collectors;
  std::vector<std::thread> collectors;
  for (size_t i = 0; i < num_of_collector; i++) {
    collectors.emplace_back([&](){
      for (size_t trial = 0; trial < collect_tials; trial++) {
        int num = rand();
        stats::ThreadLevelStatsCollector::GetCollectorForThread().CollectTestNum(num);
        actual_sum += num;
        usleep(collect_interval);
      }
    });
  }

  for (auto &collector: collectors) {
    collector.join();
  }

  finish = true;

  aggregator.join();

  ASSERT_EQ(actual_sum, aggreg_sum);

}

}
}