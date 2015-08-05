//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// tile_stats.cpp
//
// Identification: src/backend/storage/tile_stats.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/tile_stats.h"
#include "backend/storage/TableStats.h"
#include "backend/common/TupleSchema.h"
#include "backend/common/ids.h"
#include "backend/common/ValueFactory.hpp"
#include "backend/common/tabletuple.h"
#include "backend/storage/table.h"
#include "backend/storage/tablefactory.h"

#include <vector>
#include <string>

#include "backend/../stats/stats.h"

namespace peloton {
namespace storage {

vector<string> TableStats::generateTableStatsColumnNames() {
  vector<string> columnNames = StatsSource::generateBaseStatsColumnNames();
  columnNames.push_back("TABLE_NAME");
  columnNames.push_back("TABLE_TYPE");
  columnNames.push_back("TUPLE_COUNT");
  columnNames.push_back("TUPLE_ACCESSES");
  columnNames.push_back("TUPLE_ALLOCATED_MEMORY");
  columnNames.push_back("TUPLE_DATA_MEMORY");
  columnNames.push_back("STRING_DATA_MEMORY");

#ifdef ANTICACHE
  // ACTIVE
  columnNames.push_back("ANTICACHE_TUPLES_EVICTED");
  columnNames.push_back("ANTICACHE_BLOCKS_EVICTED");
  columnNames.push_back("ANTICACHE_BYTES_EVICTED");

  // GLOBAL WRITTEN
  columnNames.push_back("ANTICACHE_TUPLES_WRITTEN");
  columnNames.push_back("ANTICACHE_BLOCKS_WRITTEN");
  columnNames.push_back("ANTICACHE_BYTES_WRITTEN");

  // GLOBAL READ
  columnNames.push_back("ANTICACHE_TUPLES_READ");
  columnNames.push_back("ANTICACHE_BLOCKS_READ");
  columnNames.push_back("ANTICACHE_BYTES_READ");
#endif

  return columnNames;
}

void TableStats::populateTableStatsSchema(vector<ValueType> &types,
                                          vector<int32_t> &columnLengths,
                                          vector<bool> &allowNull) {
  StatsSource::populateBaseSchema(types, columnLengths, allowNull);
  types.push_back(VALUE_TYPE_VARCHAR);
  columnLengths.push_back(4096);
  allowNull.push_back(false);
  types.push_back(VALUE_TYPE_VARCHAR);
  columnLengths.push_back(4096);
  allowNull.push_back(false);
  types.push_back(VALUE_TYPE_BIGINT);
  columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_BIGINT));
  allowNull.push_back(false);
  types.push_back(VALUE_TYPE_BIGINT);
  columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_BIGINT));
  allowNull.push_back(false);
  types.push_back(VALUE_TYPE_INTEGER);
  columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER));
  allowNull.push_back(false);
  types.push_back(VALUE_TYPE_INTEGER);
  columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER));
  allowNull.push_back(false);
  types.push_back(VALUE_TYPE_INTEGER);
  columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER));
  allowNull.push_back(false);

#ifdef ANTICACHE
  // ANTICACHE_TUPLES_EVICTED
  types.push_back(VALUE_TYPE_INTEGER);
  columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER));
  allowNull.push_back(false);

  // ANTICACHE_BLOCKS_EVICTED
  types.push_back(VALUE_TYPE_INTEGER);
  columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER));
  allowNull.push_back(false);

  // ANTICACHE_BYTES_EVICTED
  types.push_back(VALUE_TYPE_BIGINT);
  columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_BIGINT));
  allowNull.push_back(false);

  // ANTICACHE_TUPLES_WRITTEN
  types.push_back(VALUE_TYPE_INTEGER);
  columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER));
  allowNull.push_back(false);

  // ANTICACHE_BLOCKS_WRITTEN
  types.push_back(VALUE_TYPE_INTEGER);
  columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER));
  allowNull.push_back(false);

  // ANTICACHE_BYTES_WRITTEN
  types.push_back(VALUE_TYPE_BIGINT);
  columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_BIGINT));
  allowNull.push_back(false);

  // ANTICACHE_TUPLES_READ
  types.push_back(VALUE_TYPE_INTEGER);
  columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER));
  allowNull.push_back(false);

  // ANTICACHE_BLOCKS_READ
  types.push_back(VALUE_TYPE_INTEGER);
  columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER));
  allowNull.push_back(false);

  // ANTICACHE_BYTES_READ
  types.push_back(VALUE_TYPE_BIGINT);
  columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_BIGINT));
  allowNull.push_back(false);
#endif
}

Table *TableStats::generateEmptyTableStatsTable() {
  string name = "Persistent Table aggregated table stats temp table";
  // An empty stats table isn't clearly associated with any specific
  // database ID.  Just pick something that works for now (Yes,
  // abstractplannode::databaseId(), I'm looking in your direction)
  CatalogId databaseId = 1;
  vector<string> columnNames = TableStats::generateTableStatsColumnNames();
  vector<ValueType> columnTypes;
  vector<int32_t> columnLengths;
  vector<bool> columnAllowNull;
  TableStats::populateTableStatsSchema(columnTypes, columnLengths,
                                       columnAllowNull);
  TupleSchema *schema = TupleSchema::createTupleSchema(
      columnTypes, columnLengths, columnAllowNull, true);

  return reinterpret_cast<Table *>(TableFactory::getTempTable(
      databaseId, name, schema, &columnNames[0], NULL));
}

/*
 * Constructor caches reference to the table that will be generating the
 * statistics
 */
TableStats::TableStats(Table *table)
    : StatsSource(),
      m_table(table),
      m_lastTupleCount(0),
      m_lastTupleAccessCount(0),
      m_lastAllocatedTupleMemory(0),
      m_lastOccupiedTupleMemory(0),
      m_lastStringDataMemory(0) {
#ifdef ANTICACHE
  m_lastTuplesEvicted = 0;
  m_lastBlocksEvicted = 0;
  m_lastBytesEvicted = 0;

  m_lastTuplesWritten = 0;
  m_lastBlocksWritten = 0;
  m_lastBytesWritten = 0;

  m_lastTuplesRead = 0;
  m_lastBlocksRead = 0;
  m_lastBytesRead = 0;
#endif
}

/**
 * Configure a StatsSource superclass for a set of statistics. Since this class
 * is only used in the EE it can be assumed that
 * it is part of an Execution Site and that there is a site Id.
 * @parameter name Name of this set of statistics
 * @parameter hostId id of the host this partition is on
 * @parameter hostname name of the host this partition is on
 * @parameter siteId this stat source is associated with
 * @parameter partitionId this stat source is associated with
 * @parameter databaseId Database this source is associated with
 */
void TableStats::configure(string name, CatalogId hostId, std::string hostname,
                           CatalogId siteId, CatalogId partitionId,
                           CatalogId databaseId) {
  StatsSource::configure(name, hostId, hostname, siteId, partitionId,
                         databaseId);
  m_tableName = ValueFactory::getStringValue(m_table->name());
  m_tableType = ValueFactory::getStringValue(m_table->tableType());
}

/**
 * Generates the list of column names that will be in the statTable_. Derived
 * classes must override this method and call
 * the parent class's version to obtain the list of columns contributed by
 * ancestors and then append the columns they will be
 * contributing to the end of the list.
 */
vector<string> TableStats::generateStatsColumnNames() {
  return TableStats::generateTableStatsColumnNames();
}

/**
 * Update the stats tuple with the latest statistics available to this
 * StatsSource.
 */
void TableStats::updateStatsTuple(TableTuple *tuple) {
  tuple->setNValue(StatsSource::m_columnName2Index["TABLE_NAME"], m_tableName);
  tuple->setNValue(StatsSource::m_columnName2Index["TABLE_TYPE"], m_tableType);
  int64_t tupleCount = m_table->activeTupleCount();
  int64_t tupleAccessCount = m_table->getTupleAccessCount();
  // This overflow is unlikely (requires 2 terabytes of allocated string memory)
  int64_t allocated_tuple_mem_kb = m_table->allocatedTupleMemory() / 1024;
  int64_t occupied_tuple_mem_kb = m_table->occupiedTupleMemory() / 1024;
  int64_t string_data_mem_kb = m_table->nonInlinedMemorySize() / 1024;

#ifdef ANTICACHE
  int32_t tuplesEvicted = m_table->getTuplesEvicted();
  int32_t blocksEvicted = m_table->getBlocksEvicted();
  int64_t bytesEvicted = m_table->getBytesEvicted();

  int32_t tuplesWritten = m_table->getTuplesWritten();
  int32_t blocksWritten = m_table->getBlocksWritten();
  int64_t bytesWritten = m_table->getBytesWritten();

  int32_t tuplesRead = m_table->getTuplesRead();
  int32_t blocksRead = m_table->getBlocksRead();
  int64_t bytesRead = m_table->getBytesRead();
#endif

  if (interval()) {
    tupleCount = tupleCount - m_lastTupleCount;
    m_lastTupleCount = m_table->activeTupleCount();

    tupleAccessCount = tupleAccessCount - m_lastTupleAccessCount;
    m_lastTupleAccessCount = m_table->getTupleAccessCount();

    allocated_tuple_mem_kb =
        allocated_tuple_mem_kb - (m_lastAllocatedTupleMemory / 1024);
    m_lastAllocatedTupleMemory = m_table->allocatedTupleMemory();
    occupied_tuple_mem_kb =
        occupied_tuple_mem_kb - (m_lastOccupiedTupleMemory / 1024);
    m_lastOccupiedTupleMemory = m_table->occupiedTupleMemory();
    string_data_mem_kb = string_data_mem_kb - (m_lastStringDataMemory / 1024);
    m_lastStringDataMemory = m_table->nonInlinedMemorySize();

#ifdef ANTICACHE

    // ACTIVE
    tuplesEvicted = tuplesEvicted - m_lastTuplesEvicted;
    m_lastTuplesEvicted = m_table->getTuplesEvicted();

    blocksEvicted = blocksEvicted - m_lastBlocksEvicted;
    m_lastBlocksEvicted = m_table->getBlocksEvicted();

    bytesEvicted = bytesEvicted - m_lastBytesEvicted;
    m_lastBytesEvicted = m_table->getBytesEvicted();

    // GLOBAL WRITTEN
    tuplesWritten = tuplesWritten - m_lastTuplesWritten;
    m_lastTuplesWritten = m_table->getTuplesWritten();

    blocksWritten = blocksWritten - m_lastBlocksWritten;
    m_lastBlocksWritten = m_table->getBlocksWritten();

    bytesWritten = bytesWritten - m_lastBytesWritten;
    m_lastBytesWritten = m_table->getBytesWritten();

    // GLOBAL READ
    tuplesRead = tuplesRead - m_lastTuplesRead;
    m_lastTuplesRead = m_table->getTuplesRead();

    blocksRead = blocksRead - m_lastBlocksRead;
    m_lastBlocksRead = m_table->getBlocksRead();

    bytesRead = bytesRead - m_lastBytesRead;
    m_lastBytesRead = m_table->getBytesRead();
#endif
  }

  if (string_data_mem_kb > INT32_MAX) {
    string_data_mem_kb = -1;
  }
  if (allocated_tuple_mem_kb > INT32_MAX) {
    allocated_tuple_mem_kb = -1;
  }
  if (occupied_tuple_mem_kb > INT32_MAX) {
    occupied_tuple_mem_kb = -1;
  }

  tuple->setNValue(StatsSource::m_columnName2Index["TUPLE_COUNT"],
                   ValueFactory::getBigIntValue(tupleCount));
  tuple->setNValue(StatsSource::m_columnName2Index["TUPLE_ACCESSES"],
                   ValueFactory::getBigIntValue(tupleAccessCount));

  tuple->setNValue(StatsSource::m_columnName2Index["TUPLE_ALLOCATED_MEMORY"],
                   ValueFactory::getIntegerValue(
                       static_cast<int32_t>(allocated_tuple_mem_kb)));
  tuple->setNValue(StatsSource::m_columnName2Index["TUPLE_DATA_MEMORY"],
                   ValueFactory::getIntegerValue(
                       static_cast<int32_t>(occupied_tuple_mem_kb)));
  tuple->setNValue(
      StatsSource::m_columnName2Index["STRING_DATA_MEMORY"],
      ValueFactory::getIntegerValue(static_cast<int32_t>(string_data_mem_kb)));
}

/**
 * Same pattern as generateStatsColumnNames except the return value is used as
 * an offset into the tuple schema instead of appending to
 * end of a list.
 */
void TableStats::populateSchema(vector<ValueType> &types,
                                vector<int32_t> &columnLengths,
                                vector<bool> &allowNull) {
  TableStats::populateTableStatsSchema(types, columnLengths, allowNull);
}

TableStats::~TableStats() {
  m_tableName.free();
  m_tableType.free();
}

}  // End storage namespace
}  // End peloton namespace
