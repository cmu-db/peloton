//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// stats_agent.h
//
// Identification: src/backend/stats/stats_agent.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#ifndef STATSAGENT_H_
#define STATSAGENT_H_

#include "backend/common/ids.h"
#include "backend/common/types.h"
#include <vector>
#include <map>

namespace voltdb {
class StatsSource;
class Table;

/**
 * StatsAgent serves as a central registrar for all sources of statistical
 * runtime information in an EE. In the future this could perform
 * further aggregation and processing on the collected statistics. Right now
 * statistics are only collected on persistent tables but that
 * could be extended to include stats about plan fragments and the temp tables
 * connecting them.
 */
class StatsAgent {
 public:
  /**
   * Do nothing constructor
   */
  StatsAgent();

  /**
   * Associate the specified StatsSource with the specified CatalogId under the
   * specified StatsSelector
   * @param sst Type of statistic being registered
   * @param catalogId CatalogId of the resource
   * @param statsSource statsSource containing statistics for the resource
   */
  void registerStatsSource(voltdb::StatisticsSelectorType sst,
                           voltdb::CatalogId catalogId,
                           voltdb::StatsSource *statsSource);

  /**
   * Unassociate all instances of this selector type
   */
  void unregisterStatsSource(voltdb::StatisticsSelectorType sst);

  /**
   * Get statistics for the specified resources
   * @param sst StatisticsSelectorType of the resources
   * @param catalogIds CatalogIds of the resources statistics should be
   * retrieved for
   * @param interval Return counters since the beginning or since this method
   * was last invoked
   * @param now Timestamp to return with each row
   */
  Table *getStats(voltdb::StatisticsSelectorType sst,
                  std::vector<voltdb::CatalogId> catalogIds, bool interval,
                  int64_t now);

  ~StatsAgent();

 private:
  /**
   * Map from a statistics selector to a map of CatalogIds to StatsSources.
   */
  std::map<voltdb::StatisticsSelectorType,
           std::map<voltdb::CatalogId, voltdb::StatsSource *>>
      m_statsCategoryByStatsSelector;

  /**
   * Temporary tables for aggregating the results of table statistics keyed by
   * type of statistic
   */
  std::map<voltdb::StatisticsSelectorType, voltdb::Table *>
      m_statsTablesByStatsSelector;
};
}

#endif /* STATSAGENT_H_ */
