//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// tile_stats.h
//
// Identification: src/backend/storage/tile_stats.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <string>

#include "backend/../catalog/schema.h"
#include "backend/stats/stats_source.h"
#include "backend/storage/tile.h"

namespace peloton {
namespace storage {

/// Stats for tiles
class TableStats : public stats::StatsSource {
 public:
  /**
   * Static method to generate the column names for the tables which
   * contain persistent table stats.
   */
  static std::vector<std::string> generateTableStatsColumnNames();

  /**
   * Static method to generate the remaining schema information for
   * the tables which contain persistent table stats.
   */
  static void populateTableStatsSchema(std::vector<ValueType> &types,
                                       std::vector<int32_t> &columnLengths,
                                       std::vector<bool> &allowNull);

  /**
   * Return an empty TableStats table
   */
  static Table *generateEmptyTableStatsTable();

  /*
   * Constructor caches reference to the table that will be generating the
   * statistics
   */
  TableStats(voltdb::Table *table);

  /**
   * Configure a StatsSource superclass for a set of statistics. Since this
   * class is only used in the EE it can be assumed that
   * it is part of an Execution Site and that there is a site Id.
   * @parameter name Name of this set of statistics
   * @parameter hostId id of the host this partition is on
   * @parameter hostname name of the host this partition is on
   * @parameter siteId this stat source is associated with
   * @parameter partitionId this stat source is associated with
   * @parameter databaseId Database this source is associated with
   */
  virtual void configure(std::string name, Oid hostId, std::string hostname,
                         Oid siteId, Oid partitionId, Oid databaseId);

 protected:
  /**
   * Update the stats tuple with the latest statistics available to this
   * StatsSource.
   */
  virtual void updateStatsTuple(Tuple *tuple);

  /**
   * Generates the list of column names that will be in the statTable_. Derived
   * classes must override this method and call
   * the parent class's version to obtain the list of columns contributed by
   * ancestors and then append the columns they will be
   * contributing to the end of the list.
   */
  virtual std::vector<std::string> generateStatsColumnNames();

  /**
   * Same pattern as generateStatsColumnNames except the return value is used as
   * an offset into the tuple schema instead of appending to
   * end of a list.
   */
  virtual void populateSchema(std::vector<ValueType> &types,
                              std::vector<int32_t> &columnLengths,
                              std::vector<bool> &allowNull);

  ~TableStats();

 private:
  /**
   * Table whose stats are being collected.
   */
  Table *m_table;

  Value m_tableName;

  Value m_tableType;

  int64_t m_lastTupleCount;
  int64_t m_lastTupleAccessCount;
  int64_t m_lastAllocatedTupleMemory;
  int64_t m_lastOccupiedTupleMemory;
  int64_t m_lastStringDataMemory;
};

}  // End storage namespace
}  // End peloton namespace
