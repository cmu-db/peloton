//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// stats_agent.cpp
//
// Identification: src/backend/stats/stats_agent.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/stats_agent.h"

#include "backend/common/ids.h"
#include "backend/common/tabletuple.h"
#include "backend/common/TupleSchema.h"
#include "backend/storage/PersistentTableStats.h"
#include "backend/storage/tablefactory.h"
#include "backend/storage/tableiterator.h"
#include <cassert>
#include <string>
#include <vector>

#include "backend/stats_source.h"

namespace voltdb {

StatsAgent::StatsAgent() {}

/**
 * Associate the specified StatsSource with the specified CatalogId under the
 * specified StatsSelector
 * @param sst Type of statistic being registered
 * @param catalogId CatalogId of the resource
 * @param statsSource statsSource containing statistics for the resource
 */
void StatsAgent::registerStatsSource(voltdb::StatisticsSelectorType sst,
                                     voltdb::CatalogId catalogId,
                                     voltdb::StatsSource *statsSource) {
  m_statsCategoryByStatsSelector[sst][catalogId] = statsSource;
}

void StatsAgent::unregisterStatsSource(voltdb::StatisticsSelectorType sst) {
  // get the map of id-to-source
  std::map<voltdb::StatisticsSelectorType,
           std::map<voltdb::CatalogId, voltdb::StatsSource *>>::iterator it1 =
      m_statsCategoryByStatsSelector.find(sst);

  if (it1 == m_statsCategoryByStatsSelector.end()) {
    return;
  }
  it1->second.clear();
}

/**
 * Get statistics for the specified resources
 * @param sst StatisticsSelectorType of the resources
 * @param catalogIds CatalogIds of the resources statistics should be retrieved
 * for
 * @param interval Whether to return counters since the beginning or since the
 * last time this was called
 * @param Timestamp to embed in each row
 */
Table *StatsAgent::getStats(voltdb::StatisticsSelectorType sst,
                            std::vector<voltdb::CatalogId> catalogIds,
                            bool interval, int64_t now) {
  assert(catalogIds.size() > 0);
  if (catalogIds.size() < 1) {
    return NULL;
  }
  std::map<voltdb::CatalogId, voltdb::StatsSource *> *statsSources =
      &m_statsCategoryByStatsSelector[sst];
  Table *statsTable = m_statsTablesByStatsSelector[sst];
  if (statsTable == NULL) {
    /*
     * Initialize the output table the first time.
     */
    voltdb::StatsSource *ss = (*statsSources)[catalogIds[0]];
    voltdb::Table *table = ss->getStatsTable(interval, now);
    statsTable = reinterpret_cast<Table *>(voltdb::TableFactory::getTempTable(
        table->databaseId(),
        std::string("Persistent Table aggregated stats temp table"),
        TupleSchema::createTupleSchema(table->schema()), table->columnNames(),
        NULL));
    m_statsTablesByStatsSelector[sst] = statsTable;
  }

  statsTable->deleteAllTuples(false);

  for (int ii = 0; ii < catalogIds.size(); ii++) {
    voltdb::StatsSource *ss = (*statsSources)[catalogIds[ii]];
    assert(ss != NULL);
    if (ss == NULL) {
      continue;
    }

    voltdb::TableTuple *statsTuple = ss->getStatsTuple(interval, now);
    statsTable->insertTuple(*statsTuple);
  }
  return statsTable;
}

StatsAgent::~StatsAgent() {
  for (std::map<voltdb::StatisticsSelectorType, voltdb::Table *>::iterator i =
           m_statsTablesByStatsSelector.begin();
       i != m_statsTablesByStatsSelector.end(); i++) {
    delete i->second;
  }
}
}
