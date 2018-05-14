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
#include "brain/util/eigen_util.h"
#include "catalog/catalog.h"
#include "catalog/database_catalog.h"
#include "catalog/index_catalog.h"
#include "catalog/table_catalog.h"
#include "concurrency/transaction_manager_factory.h"
#include "planner/plan_util.h"

namespace peloton {
namespace brain {

class CompressedIndexConfigContainer {
  friend class CompressedIndexConfigUtil;

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
      const std::string &database_name,
      const std::set<oid_t> &ignore_table_oids, size_t max_index_size = 3,
      bool dry_run = false, catalog::Catalog *catalog = nullptr,
      concurrency::TransactionManager *txn_manager = nullptr);

  /**
   * @brief Given a new bitset, add/drop corresponding indexes and update
   * current bitset
   */
  void AdjustIndexes(const boost::dynamic_bitset<> &new_bitset);

  // **Useful getter fns**

  /**
   * Get the global offset of an index in a table
   * @param index_obj: the index
   * @return the global offset of the index in the bitset
   */
  size_t GetGlobalOffset(
      const std::shared_ptr<brain::HypotheticalIndexObject> &index_obj) const;

  /**
 * Check whether an index is in current configuration or not
 * @param index_obj: the index to be checked
 * @return the bit for that index is set or not
 */
  bool IsSet(
      const std::shared_ptr<brain::HypotheticalIndexObject> &index_obj) const;

  /**
   * Check whether an index is in current configuration or not
   * @param offset: the global offset of the index
   * @return the bit for that index is set or not
   */
  bool IsSet(const size_t offset) const;

  /**
   * @brief Get the total number of possible indexes in current database
   */
  size_t GetConfigurationCount() const;

  /**
   * Given a global offset, get the corresponding hypothetical index object
   * @param global_offset: the global offset
   * @return the index object at "global_offset" of current configuration
   */
  std::shared_ptr<brain::HypotheticalIndexObject> GetIndex(
      size_t global_offset) const;

  /**
 * Given a global offset, get the corresponding internal index config repr
 * @param global_offset: the global offset
 * @return the internal index config mapped to this "global_offset"
 */
  std::vector<oid_t> GetIndexColumns(size_t global_offset) const;

  /**
   * @brief Get the current index configuration as a bitset(read-only)
   */
  const boost::dynamic_bitset<> *GetCurrentIndexConfig() const;

  /**
   * @brief Get instance of the txn manager
   */
  concurrency::TransactionManager *GetTransactionManager();
  /**
   * @brief Get instance of the catalog
   */
  catalog::Catalog *GetCatalog();

  std::string GetDatabaseName() const;
  /**
   * @brief Given a table oid get the bitset offset where it lies
   */
  size_t GetTableOffsetStart(oid_t table_oid) const;
  /**
   * @brief Given a table oid get the bitset offset where it ends
   */
  size_t GetTableOffsetEnd(oid_t table_oid) const;
  /**
   * @brief Given a table oid get the bitset offset the next table_oid lies.
   * Here next refers to next on the bitset
   */
  size_t GetNextTableIdx(size_t start_idx) const;
  /**
   * @brief Get the total number of indexes on a given table
   */
  size_t GetNumIndexes(oid_t table_oid) const;
  /**
   * @brief Get the next index configuration offset
   */
  size_t GetNextSetIndexConfig(size_t from_idx) const;
  /**
   * @brief Check if a table has any index config
   */
  bool EmptyConfig(oid_t table_oid) const;
  /**
   * @brief Extremely verbose representation
   */
  std::string ToString() const;
  std::string ToString(const boost::dynamic_bitset<> &bs) const;

 private:
  std::string database_name_;
  bool dry_run_;
  catalog::Catalog *catalog_;
  concurrency::TransactionManager *txn_manager_;

  /**
   * Add an index to current configuration
   * @param idx_object: the index to be added
   */
  void SetBit(const std::shared_ptr<HypotheticalIndexObject> &idx_object);

  /**
   * Add an index to current configuration
   * @param offset: the global offset of the index to be added
   */
  void SetBit(size_t offset);

  /**
   * Remove an index from current configuration
   * @param idx_object: the index to be removed
   */
  void UnsetBit(const std::shared_ptr<HypotheticalIndexObject> &idx_object);

  /**
   * Remove and index from current configuration
   * @param offset: the global offset of the index to be removed
   */
  void UnsetBit(size_t offset);

  void EnumerateConfigurations(
      const std::vector<oid_t> &cols, size_t max_index_size,
      std::map<std::vector<oid_t>, size_t> &indexconf_id_map,
      std::map<size_t, std::vector<oid_t>> &id_indexconf_map,
      std::vector<oid_t> &index_conf, size_t &next_id);

  /**
   * Outer mapping: table_oid -> inner mapping
   * Inner mapping: column_oid -> internal mapping ID
   *
   * For example, table T (table_oid = 12345) has three columns: A (column_oid =
   * 5), B (column_oid = 3), C (column_oid = 14). Then we will have:
   * table_id_map_[12345] ==> inner mapping
   * inner mapping ==> {Nothing->0, {5}->1, {3}->2, {14}-> 3, {5, 3} -> 4....
   * Basically every possible single and multicol index ordering gets a unique
   * identifier.
   * Identifiers continue when we go from one table to the next - i.e. if table
   * T1 ends at id 15
   * Table T2 starts at 16 and goes on from there.
   * TODO(saatviks): Come up with an even more compressed rep.(like eg. a->0,
   * b->1, c->2
   * and Nothing = 000, {a} = 001, {ab} = 011, etc. Problem is this doesnt work
   * for
   * permutations - only for combinations).
   */
  std::unordered_map<oid_t, std::map<std::vector<oid_t>, size_t>>
      table_indexid_map_;

  /**
   * Outer mapping: table_oid -> inner reverse mapping
   * Inner reverse mapping is the reverse of `inner mapping`
   * explained above
   */
  std::unordered_map<oid_t, std::map<size_t, std::vector<oid_t>>>
      indexid_table_map_;

  /**
   * In order to enable faster table->col lookups we also store table offsets
   * separately.
   * This also allows for other functionality.
   */
  std::map<oid_t, size_t> table_offset_map_;

  // This map is just the reverse mapping of table_offset_map_
  std::map<size_t, oid_t> table_offset_reverse_map_;

  // This map stores global offset -> index's oid
  std::unordered_map<size_t, oid_t> offset_to_indexoid_;

  // the next offset of a new table(during construction)
  // the end pointer - post construction
  size_t next_table_offset_;

  std::unique_ptr<boost::dynamic_bitset<>> cur_index_config_;
};
}  // namespace brain
}  // namespace peloton
