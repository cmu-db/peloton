//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// zone_map_manager.cpp
//
// Identification: src/storage/zone_map_manager.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/zone_map_manager.h"

#include "catalog/catalog.h"
#include "catalog/zone_map_catalog.h"
#include "catalog/database_catalog.h"
#include "concurrency/transaction_context.h"
#include "concurrency/transaction_manager_factory.h"
#include "storage/storage_manager.h"
#include "storage/data_table.h"
#include "type/ephemeral_pool.h"

namespace peloton {
namespace storage {

/**
 * @brief Get instance of the global zone map manager
 */
ZoneMapManager *ZoneMapManager::GetInstance() {
  static ZoneMapManager global_zone_map_manager;
  return &global_zone_map_manager;
}

ZoneMapManager::ZoneMapManager() {
  zone_map_table_exists = false;
  pool_.reset(new type::EphemeralPool());
}

/**
 * @brief The function creates the zone map table in catalog
 */
void ZoneMapManager::CreateZoneMapTableInCatalog() {
  LOG_DEBUG("Create the Zone Map table");
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::ZoneMapCatalog::GetInstance(txn);
  txn_manager.CommitTransaction(txn);
  zone_map_table_exists = true;
}

/**
 * @brief The function creates zone maps for all tile groups for a given table
 *
 * @param table The table we're creating the zone map for
 * @param txn The transaction handle under which we create the zone map
 */
void ZoneMapManager::CreateZoneMapsForTable(
    storage::DataTable *table, concurrency::TransactionContext *txn) {
  PELOTON_ASSERT(table != nullptr);
  // Scan over the tile groups, check for immutable flag to be true
  // and keep adding to the zone map catalog
  size_t num_tile_groups = table->GetTileGroupCount();
  for (size_t i = 0; i < num_tile_groups; i++) {
    auto tile_group = table->GetTileGroup(i);
    auto tile_group_ptr = tile_group.get();
    PELOTON_ASSERT(tile_group_ptr != nullptr);
    auto tile_group_header = tile_group_ptr->GetHeader();
    PELOTON_ASSERT(tile_group_header != nullptr);
    bool immutable = tile_group_header->GetImmutability();
    if (immutable) {
      CreateOrUpdateZoneMapForTileGroup(table, i, txn);
    }
  }
}

/**
 * @brief The function creates zone maps for a given tile group. If it already
 * exists it is deleted and replaced with the updated values.
 *
 * @param table The table we're creating the zone map for
 * @param tile_group_idx The ID of the tile group we're creating the zone map
 * @param txn The transaction handle under which we create the zone map
 */
void ZoneMapManager::CreateOrUpdateZoneMapForTileGroup(
    storage::DataTable *table, oid_t tile_group_idx,
    concurrency::TransactionContext *txn) {
  LOG_DEBUG("Creating Zone Maps for TileGroupId : %u", tile_group_idx);

  oid_t database_id = table->GetDatabaseOid();
  oid_t table_id = table->GetOid();
  auto schema = table->GetSchema();
  size_t num_columns = schema->GetColumnCount();
  auto tile_group = table->GetTileGroup(tile_group_idx);

  for (oid_t col_itr = 0; col_itr < num_columns; col_itr++) {
    // Set temp min and temp max as the first value.
    type::Value min = tile_group->GetValue(0, col_itr);
    type::Value max = tile_group->GetValue(0, col_itr);
    // Iterate over all tuples in this column
    oid_t num_tuple_slots = tile_group->GetAllocatedTupleCount();
    for (oid_t tuple_itr = 0; tuple_itr < num_tuple_slots; tuple_itr++) {
      type::Value current_val = tile_group->GetValue(tuple_itr, col_itr);
      if (current_val.CompareGreaterThan(max) == CmpBool::CmpTrue) {
        max = current_val;
      }
      if (current_val.CompareLessThan(min) == CmpBool::CmpTrue) {
        min = current_val;
      }
    }
    type::TypeId val_type = min.GetTypeId();
    std::string converted_min = min.ToString();
    std::string converted_max = max.ToString();
    std::string converted_type = TypeIdToString(val_type);

    CreateOrUpdateZoneMapInCatalog(database_id, table_id, tile_group_idx,
                                   col_itr, converted_min, converted_max,
                                   converted_type, txn);
  }
}

/**
 * @brief The function inserts the catalog keys and values in the zone map table
 * present in catalog
 *
 * @param database_id The ID of the database the table belongs to
 * @param table_id The ID of the table the zonemap is for
 * @param tile_group_idx The ID of the tile group the zone map is for
 * @param column_id The ID of the column we're creating stats for
 * @param min The minimum value of the column in the tile group
 * @param max The maxmimum value of the column in the tile group
 * @param type The type of column
 * @param txn The transaciton handle used to insert into the catalog
 */
void ZoneMapManager::CreateOrUpdateZoneMapInCatalog(
    oid_t database_id, oid_t table_id, oid_t tile_group_idx, oid_t column_id,
    std::string min, std::string max, std::string type,
    concurrency::TransactionContext *txn) {
  auto stats_catalog = catalog::ZoneMapCatalog::GetInstance(nullptr);
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  // Delete and update in a single commit.
  bool single_statement_txn = false;
  if (txn == nullptr) {
    single_statement_txn = true;
    txn = txn_manager.BeginTransaction();
  }
  stats_catalog->DeleteColumnStatistics(txn,
                                        database_id,
                                        table_id,
                                        tile_group_idx,
                                        column_id);

  stats_catalog->InsertColumnStatistics(txn,
                                        database_id,
                                        table_id,
                                        tile_group_idx,
                                        column_id,
                                        min,
                                        max,
                                        type,
                                        pool_.get());

  if (single_statement_txn) {
    txn_manager.CommitTransaction(txn);
  }
}

/**
 * Retrieves column statistics for the given column from the catalog.
 *
 * @param database_id The ID of the database the table belongs to
 * @param table_id The ID of the table the zonemap is for
 * @param tile_group_idx The ID of the tile group the zone map is for
 * @param column_id The ID of the column we're creating stats for
 *
 * @return  unique pointer to the column statistics for a given column
 */
std::unique_ptr<ZoneMapManager::ColumnStatistics>
ZoneMapManager::GetZoneMapFromCatalog(oid_t database_id, oid_t table_id,
                                      oid_t tile_group_idx, oid_t column_id) {
  auto stats_catalog = catalog::ZoneMapCatalog::GetInstance(nullptr);
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  auto result_vector = stats_catalog->GetColumnStatistics(txn,
                                                          database_id,
                                                          table_id,
                                                          tile_group_idx,
                                                          column_id);
  txn_manager.CommitTransaction(txn);

  if (result_vector == nullptr) {
    return nullptr;
  }
  return GetResultVectorAsZoneMap(result_vector);
}

/**
 * The function performs deserialization back from VARCHAR
 *
 * @param[out] result_vector the result tile obtained from catalog after lookup
 *
 * @return  unique pointer to the column statistics for a given column
 */
std::unique_ptr<ZoneMapManager::ColumnStatistics>
ZoneMapManager::GetResultVectorAsZoneMap(
    std::unique_ptr<std::vector<type::Value>> &result_vector) {
  type::Value min_varchar = (*result_vector)[static_cast<int>(
      catalog::ZoneMapCatalog::ZoneMapOffset::MINIMUM_OFF)];
  type::Value max_varchar = (*result_vector)[static_cast<int>(
      catalog::ZoneMapCatalog::ZoneMapOffset::MAXIMUM_OFF)];
  type::Value type_varchar = (*result_vector)[static_cast<int>(
      catalog::ZoneMapCatalog::ZoneMapOffset::TYPE_OFF)];

  return std::unique_ptr<ZoneMapManager::ColumnStatistics>(
      new ZoneMapManager::ColumnStatistics(
          {GetValueAsOriginal(min_varchar, type_varchar),
           GetValueAsOriginal(max_varchar, type_varchar)}));
}

/**
 * The function compares the predicate against the zone map for the column
 * present in catalog.
 *
 * @param parsed predicates array
 * @param num_predicates
 * @param table
 * @param tile_group_idx
 *
 * @return  True if tile group needs to be scanned, false if it can be skipped
 */
bool ZoneMapManager::ShouldScanTileGroup(
    storage::PredicateInfo *parsed_predicates, int32_t num_predicates,
    storage::DataTable *table, int64_t tile_group_idx) {
  for (int32_t i = 0; i < num_predicates; i++) {
    // Extract the col_id, operator and predicate_value
    int col_id = parsed_predicates[i].col_id;
    int comparison_operator = parsed_predicates[i].comparison_operator;
    type::Value predicate_value = parsed_predicates[i].predicate_value;

    oid_t database_id = table->GetDatabaseOid();
    oid_t table_id = table->GetOid();

    std::unique_ptr<ZoneMapManager::ColumnStatistics> stats =
        GetZoneMapFromCatalog(database_id, table_id, tile_group_idx, col_id);

    if (stats == nullptr) {
      return true;
    }
    switch (comparison_operator) {
      case (int)ExpressionType::COMPARE_EQUAL:
        if (!CheckEqual(predicate_value, stats.get())) {
          return false;
        }
        break;
      case (int)ExpressionType::COMPARE_LESSTHAN:
        if (!CheckLessThan(predicate_value, stats.get())) {
          return false;
        }
        break;
      case (int)ExpressionType::COMPARE_LESSTHANOREQUALTO:
        if (!CheckLessThanEquals(predicate_value, stats.get())) {
          return false;
        }
        break;
      case (int)ExpressionType::COMPARE_GREATERTHAN:
        if (!CheckGreaterThan(predicate_value, stats.get())) {
          return false;
        }
        break;
      case (int)ExpressionType::COMPARE_GREATERTHANOREQUALTO:
        if (!CheckGreaterThanEquals(predicate_value, stats.get())) {
          return false;
        }
        break;
      default: { throw Exception{"Invalid expression type for translation "}; }
    }
  }
  return true;
}

/**
 * Checks whether a zone map table in catalog was created.
 *
 * @return  True if the zone map table in catalog exists and vice versa
 */
bool ZoneMapManager::ZoneMapTableExists() { return zone_map_table_exists; }

}  // namespace storage
}  // namespace peloton