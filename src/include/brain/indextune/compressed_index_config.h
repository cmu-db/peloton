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
#include "util/file_util.h"

namespace peloton {
namespace brain {

// TODO: Maybe we should rename it to CompressedIndexConfigUtil
// TODO: Maybe we should decouple the Manager and the bitset based
// CompressedIndexConfig

class CompressedIndexConfiguration {
 public:
  /**
   * Constructor for CompressedIndexConfiguration: Initialize
   * (1) catalog pointer
   * (2) txn_manager pointer
   * One such configuration is for only one database.
   *
   * Then scan all the tables in the database to populate the internal maps
   * Finally, scan all tables again to generate current index configuration (a
   * bitset)
   */
  explicit CompressedIndexConfiguration(
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
   * Given a global offset, get the corresponding index
   * @param global_offset: the global offset
   * @return the index object at "global_offset" of current configuration
   */
  std::shared_ptr<brain::IndexObject> GetIndex(size_t global_offset) const;

  /**
   * Add an index to current configuration
   * @param idx_object: the index to be added
   */
  void AddIndex(const std::shared_ptr<IndexObject> &idx_object);

  /**
   * Add an index to current configuration
   * @param offset: the global offset of the index to be added
   */
  void AddIndex(size_t offset);

  /**
   * Remove an index from current configuration
   * @param idx_object: the index to be removed
   */
  void RemoveIndex(const std::shared_ptr<IndexObject> &idx_object);

  /**
   * Remove and index from current configuration
   * @param offset: the global offset of the index to be removed
   */
  void RemoveIndex(size_t offset);

  /**
   * Given an index configuration, generate the prefix closure
   * @param indexes: the index configuration
   * @return the prefix closure as a bitset
   */
  std::unique_ptr<boost::dynamic_bitset<>> AddDropCandidate(
      const IndexConfiguration &indexes);

  /**
   * Given a SQLStatementList, generate the prefix closure from the first
   * SQLStatement element
   * @param sql_stmt_list: the SQLStatementList
   * @return the prefix closure as a bitset
   */
  std::unique_ptr<boost::dynamic_bitset<>> AddDropCandidate(
      std::unique_ptr<parser::SQLStatementList> sql_stmt_list);

  /**
   * @brief Get the total number of possible indexes in current database
   */
  size_t GetConfigurationCount();

  /**
   * @brief Get the current index configuration as a bitset
   */
  const boost::dynamic_bitset<> *GetCurrentIndexConfig();

  /**
   * @brief Get the Eigen vector representation of the current index config bitset
   */
  void GetEigen(vector_eig& curr_config_vec);

  std::string ToString();



 private:
  std::string database_name_;
  catalog::Catalog *catalog_;
  concurrency::TransactionManager *txn_manager_;

  std::unordered_map<oid_t, std::unordered_map<oid_t, size_t>> table_id_map_;
  std::unordered_map<oid_t, std::unordered_map<size_t, oid_t>> id_table_map_;
  std::map<oid_t, size_t> table_offset_map_;
  std::map<size_t, oid_t> table_offset_reverse_map_;

  size_t next_table_offset_;

  std::unique_ptr<boost::dynamic_bitset<>> cur_index_config_;

  void AddIndex(std::unique_ptr<boost::dynamic_bitset<>> &bitmap,
                const std::shared_ptr<IndexObject> &idx_object);

  void AddIndex(std::unique_ptr<boost::dynamic_bitset<>> &bitmap,
                size_t offset);
};
}
}
