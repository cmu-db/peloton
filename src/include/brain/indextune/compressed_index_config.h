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
#include "util/file_util.h"

namespace peloton {
namespace brain {

class CompressedIndexConfiguration {
 public:
  explicit CompressedIndexConfiguration(
      catalog::Catalog *catalog, concurrency::TransactionManager *txn_manager);

  // Create a new database
  void CreateDatabase(const std::string &db_name);

  // Create a new table with schema (a INT, b INT, c INT).
  // TODO: modify
  void CreateTable(const std::string &table_name);

  // TODO: remove
  void CreateIndex_A(const std::string &table_name);

  // TODO: remove
  void CreateIndex_B(const std::string &table_name);

  void DropTable(const std::string &table_name);

  void DropDatabase();

  size_t GetLocalOffset(const oid_t table_oid,
                        const std::set<oid_t> &column_oids);

  size_t GetGlobalOffset(const std::shared_ptr<brain::IndexObject> &index_obj);

  bool IsSet(const std::shared_ptr<boost::dynamic_bitset<>> &bitset,
             const std::shared_ptr<brain::IndexObject> &index_obj);

  void Set(const std::shared_ptr<boost::dynamic_bitset<>> &bitset,
           const std::shared_ptr<brain::IndexObject> &index_obj);

  std::shared_ptr<boost::dynamic_bitset<>> GenerateCurrentBitSet();

  void AddIndex(const std::shared_ptr<IndexObject> &idx_object);

  void AddIndex(size_t offset);

  void RemoveIndex(const std::shared_ptr<IndexObject> &idx_object);

  void RemoveIndex(size_t offset);

  std::shared_ptr<boost::dynamic_bitset<>> AddCandidate(
      const IndexConfiguration &indexes);

  std::shared_ptr<boost::dynamic_bitset<>> DropCandidate(
      const IndexConfiguration &indexes);



 private:
  std::string database_name_;
  catalog::Catalog *catalog_;
  concurrency::TransactionManager *txn_manager_;

  std::unordered_map<oid_t, std::unordered_map<oid_t, size_t>> table_id_map_;
  std::unordered_map<oid_t, std::unordered_map<size_t, oid_t>> id_table_map_;
  std::unordered_map<oid_t, size_t> table_offset_map_;

  size_t next_table_offset_;
};
}
}