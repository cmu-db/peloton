//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// stats_channel_test.cpp
//
// Identification: test/statistics/stats_channel_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "statistics/stats_channel.h"
#include "common/harness.h"
#include "statistics/oid_aggr_reducer.h"

namespace peloton {
namespace test {

class StatsChannelTests : public PelotonTest {};

TEST_F(StatsChannelTests, OidStatsChannelTests) {
  stats::StatsChannel<oid_t, stats::OidAggrReducer> channel(20);
  std::unordered_set<oid_t> oid_set;
  stats::OidAggrReducer reducer(oid_set);

  channel.AddMessage(1);
  channel.Reduce(reducer);
  ASSERT_EQ(oid_set.size(), 1);
  ASSERT_EQ(oid_set.count(1), 1);

  channel.AddMessage(1);
  channel.Reduce(reducer);
  ASSERT_EQ(oid_set.size(), 1);
  ASSERT_EQ(oid_set.count(1), 1);

  channel.AddMessage(2);
  channel.AddMessage(3);
  channel.Reduce(reducer);
  ASSERT_EQ(oid_set.size(), 3);
  ASSERT_EQ(oid_set.count(3), 1);
  ASSERT_EQ(oid_set.count(2), 1);
}

TEST_F(StatsChannelTests, OidStatsChannelConcurrentTests) {
  std::vector<std::thread> threads;
  stats::StatsChannel<oid_t, stats::OidAggrReducer> channel(100);
  for (oid_t i = 0; i < 100; i++) {
    threads.emplace_back([i, &channel]() { channel.AddMessage(i); });
  }

  for (oid_t i = 0; i < 100; i++) {
    threads[i].join();
  }

  std::unordered_set<oid_t> oid_set;
  stats::OidAggrReducer reducer(oid_set);
  channel.Reduce(reducer);

  ASSERT_EQ(oid_set.size(), 100);
  for (oid_t i = 0; i < 100; i++) {
      ASSERT_EQ(oid_set.count(i), 1);
  }
}

}  // namespace test
}  // namespace peloton