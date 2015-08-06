//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// stats_source.h
//
// Identification: src/backend/stats/stats_source.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/value.h"
#include "backend/common/types.h"
#include "backend/storage/tuple.h"
#include "backend/storage/table.h"

#include <string>
#include <vector>
#include <map>

namespace peloton {
namespace stats {

class Table;
class TableFactory;
class Schema;
class Tuple;

//===--------------------------------------------------------------------===//
// Base class for all statistical sources.
//===--------------------------------------------------------------------===//

/**
 * Statistics are currently represented as a single table
 * that is updated every time it is retrieved.
 */
class StatsSource {
  StatsSource() = delete;
  StatsSource(StatsSource const &) = delete;

 public:
  /**
   * Generates the list of column names that are present for every
   * stats table.
   *
   * Derived classes should implement their own static
   * methods to generate their column names and call this method
   * within it to populate the column name vector before adding
   * their stat-specific column names.
   */
  static std::vector<std::string> GetBaseStatsTableColumnNames();

  /**
   * Populates the other schema information which is present for
   * every stats table.
   */
  std::vector<catalog::Column> CreateBaseStatsTableSchema();

  /// Configure for a set of statistics.
  void Configure(std::string stats_identifier, Oid host_id, Oid site_id,
                 Oid database_id);

  virtual ~StatsSource();

  /**
   * Retrieve table containing the latest statistics available.
   * An updated stat is requested from the derived class by calling
   *updateStatsTuple
   *
   * @param interval Return counters since the beginning or since this method
   *was last invoked
   * @param now Timestamp to return with each row
   * @return Pointer to a table containing the statistics.
   */
  Table *GetTable(bool interval, int64_t timestamp);

  /*
   * Retrieve tuple containing the latest statistics available.
   * An updated stat is requested from the derived class by calling
   *updateStatsTuple
   *
   * @param interval Whether to return counters since the beginning or since the
   *last time this was called
   * @param Timestamp to embed in each row
   * @return Pointer to a table tuple containing the latest version of the
   *statistics.
   */
  storage::Tuple *GetTuple(bool interval, int64_t timestamp);

  std::string GetIdentifier() { return identifier; }

  /// String representation of the statistics. Default implementation is to
  /// print the stats table.
  friend std::ostream &operator<<(std::ostream &os,
                                  const StatsSource &stats_source);

 protected:
  /**
   * Update the stats tuple with the latest statistics available to this
   * StatsSource.
   * Implemented by derived classes.
   * @parameter tuple TableTuple pointing to a row in the stats table.
   */
  virtual void UpdateTuple(storage::Tuple *tuple) = 0;

  /**
   * Generates the list of column names that will be in the statTable.
   *
   * Derived classes must override this method and call
   *@GetBaseStatsTableColumns
   * to obtain columns contributed by ancestors and then append the columns they
   *will be
   * contributing to the end of the list.
   */
  virtual std::vector<std::string> GetStatsTableColumnNames();

  /**
   * Same pattern as generateStatsColumnNames except the return value is used as
   * an offset into the tuple schema instead of appending to end of a list.
   */
  virtual std::vector<catalog::Column> CreateStatsTableSchema();

  /**
   * Map describing the mapping from column names to column indices in the stats
   * tuple.
   * Necessary because classes in the inheritance hierarchy can vary the number
   * of columns
   * they contribute. This removes the dependency between them.
   */
  std::map<std::string, int> column_name_to_index;

  bool GetInterval() { return interval; }

 private:
  /// Table containing the stats
  storage::Table *table;

  /// Tuple used to modify the stats table.
  storage::Tuple tuple;

  /// Name of this set of statistics.
  std::string identifier;

  /// CatalogIds
  Oid host_id;
  Oid site_id;
  Oid database_id;

  std::string host_name;

  bool interval;
};

}  // End stats namespace
}  // End peloton namespace
