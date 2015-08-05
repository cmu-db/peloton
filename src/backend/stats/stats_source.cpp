//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// stats_source.cpp
//
// Identification: src/backend/stats/stats_source.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/stats/stats_source.h"

#include "backend/common/value_factory.h"
#include "backend/storage/table.h"
#include "backend/storage/table_factory.h"

#include <vector>
#include <string>
#include <cassert>

namespace peloton {
namespace stats {

std::vector<std::string> StatsSource::GetBaseStatsTableColumnNames() {
  std::vector<std::string> column_names;

  column_names.push_back("TIMESTAMP");
  column_names.push_back("HOST_ID");
  column_names.push_back("HOSTNAME");
  column_names.push_back("SITE_ID");
  column_names.push_back("PARTITION_ID");

  return column_names;
}

std::vector<catalog::Column> StatsSource::CreateBaseStatsTableSchema() {
  std::vector<catalog::Column> columns;

  catalog::Column column1(VALUE_TYPE_BIGINT, GetTypeSize(VALUE_TYPE_BIGINT),
                          false);
  catalog::Column column2(VALUE_TYPE_BIGINT, GetTypeSize(VALUE_TYPE_BIGINT),
                          false);
  catalog::Column column3(VALUE_TYPE_VARCHAR, VARCHAR_LENGTH_LONG, false);
  catalog::Column column4(VALUE_TYPE_BIGINT, GetTypeSize(VALUE_TYPE_BIGINT),
                          false);
  catalog::Column column5(VALUE_TYPE_BIGINT, GetTypeSize(VALUE_TYPE_BIGINT),
                          false);

  columns.push_back(column1);
  columns.push_back(column2);
  columns.push_back(column3);
  columns.push_back(column4);
  columns.push_back(column5);

  return columns;
}

/// Configure for a set of statistics.
void StatsSource::Configure(std::string _identifier, Oid _host_id, Oid _site_id,
                            Oid database_id) {
  host_id = _host_id;
  site_id = _site_id;
  identifier = _identifier;

  std::vector<std::string> column_names = GetBaseStatsTableColumnNames();
  std::vector<catalog::Column> columns;

  columns = CreateBaseStatsTableSchema();

  Schema *schema = Schema::Schema(columns);

  for (int column_itr = 0; column_itr < column_names.size(); column_itr++) {
    column_name_to_index[column_names[column_itr]] = column_itr;
  }

  table->Reset(TableFactory::GetTempTable(database_id, identifier, schema,
                                          column_names, 0));

  tuple = table->TempTuple();
}

/**
 * Retrieve table containing the latest statistics available.
 * An updated stat is requested from the derived class by calling
 *updateStatsTuple
 *
 * @param interval Return counters since the beginning or since this method was
 *last invoked
 * @param now Timestamp to return with each row
 * @return Pointer to a table containing the statistics.
 */
Table *StatsSource::GetTable(bool interval, int64_t timestamp) {
  GetTuple(interval, timestamp);

  return table->Get();
}

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
storage::Tuple *StatsSource::GetTuple(bool _interval, int64_t timestamp) {
  interval = _interval;
  assert(table != NULL);

  tuple.SetValue(0, ValueFactory::GetBigIntValue(timestamp));
  tuple.SetValue(1, ValueFactory::GetBigIntValue(host_id));
  tuple.SetValue(2, ValueFactory::GetStringValue(host_name));
  tuple.SetValue(3, ValueFactory::GetBigIntValue(site_id));

  UpdateTuple(&tuple);

  table->InsertTuple(tuple);

  return &tuple;
}

/**
 * Generates the list of column names that will be in the statTable.
 *
 * Derived classes must override this method and call @GetBaseStatsTableColumns
 * to obtain columns contributed by ancestors and then append the columns they
 *will be
 * contributing to the end of the list.
 */
std::vector<std::string> StatsSource::GetStatsTableColumnNames() {
  return StatsSource::GetBaseStatsTableColumnNames();
}

/// String representation of the statistics. Default implementation is to print
/// the stats table.
std::ostream &operator<<(std::ostream &os, const StatsSource &stats_source) {
  uint32_t column_count;

  column_count = stats_source.table->GetColumnCount();

  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    os << stats_source.table->GetColumnName(column_itr);
    os << "\t";
  }

  os << "\n";
  os << stats_source.tuple;

  return os;
}

/**
 * Same pattern as generateStatsColumnNames except the return value is used as
 * an offset into the tuple schema instead of appending to end of a list.
 */
std::vector<catalog::Column> StatsSource::CreateStatsTableSchema() {
  return StatsSource::CreateBaseStatsTableSchema();
}

}  // End stats namespace
}  // End peloton namespace
