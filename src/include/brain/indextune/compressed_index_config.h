//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// compressed_index_config.h
//
// Identification: src/include/brain/indextune/compressed_index_config.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <boost/dynamic_bitset.hpp>
#include "brain/index_selection.h"
#include "catalog/catalog.h"
#include "catalog/database_catalog.h"
#include "catalog/index_catalog.h"
#include "catalog/table_catalog.h"
#include "concurrency/transaction_manager_factory.h"
#include "brain/util/eigen_util.h"
#include "planner/plan_util.h"

namespace peloton {
namespace brain {

class CompressedIndexConfigContainer {
  friend class CompressedIndexConfigManager;

 public:
  /**
   * Constructor for CompressedIndexConfigContainer: Initialize
   * (1) catalog pointer
   * (2) txn_manager pointer
   * One such configuration is for only one database.
   *
   * Then scan all the tables in the database to populate the internal maps
   * Finally, scan all tables again to generate current index configuration (a
   * bitset)
   */
  explicit CompressedIndexConfigContainer(
      const std::string &database_name, catalog::Catalog *catalog = nullptr,
      concurrency::TransactionManager *txn_manager = nullptr);

  /**
   * Get the local offset of an index in a table
   * @param table_oid: the table oid
   * @param column_oids: a set of column oids, representing the index
   * @return the local offset of the index in the bitset
   */
  size_t GetLocalOffset(const oid_t table_oid,
                        const std::set<oid_t> &column_oids) const;

  /**
   * Get the global offset of an index in a table
   * @param index_obj: the index
   * @return the global offset of the index in the bitset, which is "table
   * offset" + "local offset"
   */
  size_t GetGlobalOffset(
      const std::shared_ptr<brain::IndexObject> &index_obj) const;

  /**
   * Check whether an index is in current configuration or not
   * @param index_obj: the index to be checked
   * @return the bit for that index is set or not
   */
  bool IsSet(const std::shared_ptr<brain::IndexObject> &index_obj) const;

  /**
  * Check whether an index is in current configuration or not
  * @param offset: the global offset of the index
  * @return the bit for that index is set or not
  */
  bool IsSet(const size_t offset) const;

  /**
   * Given a global offset, get the corresponding index
   * @param global_offset: the global offset
   * @return the index object at "global_offset" of current configuration
   */
  std::shared_ptr<brain::IndexObject> GetIndex(size_t global_offset) const;

  /**
   * Add an index to current configuration
   * @param idx_object: the index to be added
   */
  void SetBit(const std::shared_ptr<IndexObject> &idx_object);

  /**
   * Add an index to current configuration
   * @param offset: the global offset of the index to be added
   */
  void SetBit(size_t offset);

  /**
   * Remove an index from current configuration
   * @param idx_object: the index to be removed
   */
  void UnsetBit(const std::shared_ptr<IndexObject> &idx_object);

  /**
   * Remove and index from current configuration
   * @param offset: the global offset of the index to be removed
   */
  void UnsetBit(size_t offset);

  // Getters
  /**
   * @brief Get the total number of possible indexes in current database
   */
  size_t GetConfigurationCount() const;

  /**
   * @brief Get the current index configuration as a bitset(read-only)
   */
  const boost::dynamic_bitset<> *GetCurrentIndexConfig() const;
  concurrency::TransactionManager *GetTransactionManager();
  catalog::Catalog *GetCatalog();
  std::string GetDatabaseName() const;
  size_t GetTableOffset(oid_t table_oid) const;

  // Utility functions
  std::string ToString() const;
  /**
 * @brief Get the Eigen vector/feature representation of the current index
 * @param container: input container
 * config bitset
 */
  void ToEigen(vector_eig &config_vec) const;

  /**
   * @brief Get the Eigen vector/feature representation of the covered index
   * config
   */
  void ToCoveredEigen(vector_eig &config_vec) const;

 private:
  std::string database_name_;
  catalog::Catalog *catalog_;
  concurrency::TransactionManager *txn_manager_;

  /**
   * Outer mapping: table_oid -> inner mapping
   * Inner mapping: column_oid -> internal mapping ID
   *
   * For example, table T (table_oid = 12345) has three columns: A (column_oid =
   * 5), B (column_oid = 3), C (column_oid = 14). Then we will have:
   * table_id_map_[12345] ==> inner mapping
   * inner mapping ==> {5->0, 3->1, 14, 2} (here 5, 3 and 14 are column oids, 0,
   * 1 and 2 are interal mapping IDs)
   */
  std::unordered_map<oid_t, std::unordered_map<oid_t, size_t>> table_id_map_;

  /**
   * Outer mapping: table_oid -> inner reverse mapping
   * Inner reverse mapping: internal mapping ID -> column_oid
   *
   * Using the same example as above, now we will have:
   * table_id_map_[12345] ==> inner reverse mapping
   * inner revserse mapping ==> {0->5, 1->3, 2->14} (here 5, 3 and 14 are column
   * oids, 0, 1 and 2 are interal mapping IDs)
   */
  std::unordered_map<oid_t, std::unordered_map<size_t, oid_t>> id_table_map_;

  /**
   * the mapping between table_oid and the starting position of table in the
   * bitset.
   *
   * For example, table A (table_oid = 111) has 3 columns (8 possible index
   * configs in total), table B (table_oid =
   * 222) has 2 columns (4 possible index configs in total), table C (table_oid
   * = 333) has 4 columns (16 possible index configs in total).
   *
   * Then we will have:
   * table_offset_map_[111] = 0
   * table_offset_map_[222] = 8
   * table_offset_map_[333] = 12
   */
  std::map<oid_t, size_t> table_offset_map_;

  // This map is just the reverse mapping of table_offset_map_
  std::map<size_t, oid_t> table_offset_reverse_map_;

  // This map stores an index's oid -> its global offset in the bitset
  std::unordered_map<oid_t, size_t> index_id_map_;

  // This map is the reverse mapping of index_id_map_
  std::unordered_map<size_t, oid_t> index_id_reverse_map_;

  // the next offset of a new table
  size_t next_table_offset_;

  std::unique_ptr<boost::dynamic_bitset<>> cur_index_config_;
};
}
}
