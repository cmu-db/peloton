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

// TODO: Maybe we should rename it to CompressedIndexConfigManager
// TODO: Maybe we should decouple the Manager and the bitset based
// CompressedIndexConfig

class CompressedIndexConfiguration {
 public:
  explicit CompressedIndexConfiguration(
      const std::string &database_name, catalog::Catalog *catalog = nullptr,
      concurrency::TransactionManager *txn_manager = nullptr);

  size_t GetLocalOffset(const oid_t table_oid,
                        const std::set<oid_t> &column_oids) const;

  size_t GetGlobalOffset(
      const std::shared_ptr<brain::IndexObject> &index_obj) const;

  bool IsSet(const std::shared_ptr<brain::IndexObject> &index_obj) const;

  std::shared_ptr<brain::IndexObject> GetIndex(size_t global_offset) const;

  void AddIndex(const std::shared_ptr<IndexObject> &idx_object);

  void AddIndex(size_t offset);

  void RemoveIndex(const std::shared_ptr<IndexObject> &idx_object);

  void RemoveIndex(size_t offset);

  std::shared_ptr<boost::dynamic_bitset<>> AddDropCandidate(
      const IndexConfiguration &indexes);

  size_t GetConfigurationCount();

  const std::shared_ptr<boost::dynamic_bitset<>> GetCurrentIndexConfig();

 private:
  std::string database_name_;
  catalog::Catalog *catalog_;
  concurrency::TransactionManager *txn_manager_;

  std::unordered_map<oid_t, std::unordered_map<oid_t, size_t>> table_id_map_;
  std::unordered_map<oid_t, std::unordered_map<size_t, oid_t>> id_table_map_;
  std::map<oid_t, size_t> table_offset_map_;
  std::map<size_t, oid_t> table_offset_reverse_map_;

  size_t next_table_offset_;
  std::shared_ptr<boost::dynamic_bitset<>> cur_index_config_;

  void AddIndex(std::shared_ptr<boost::dynamic_bitset<>> &bitmap,
                const std::shared_ptr<IndexObject> &idx_object);

  void AddIndex(std::shared_ptr<boost::dynamic_bitset<>> &bitmap,
                size_t offset);
};
}
}
