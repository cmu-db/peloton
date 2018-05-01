//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// compressed_index_config.cpp
//
// Identification: src/brain/indextune/compressed_index_config.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/indextune/compressed_index_config.h"

namespace peloton {
namespace brain {

CompressedIndexConfiguration::CompressedIndexConfiguration(
    const std::string &database_name, catalog::Catalog *catalog,
    concurrency::TransactionManager *txn_manager)
    : database_name_{database_name},
      catalog_{catalog},
      txn_manager_{txn_manager},
      next_table_offset_{0},
      cur_index_config_{nullptr} {
  if (nullptr == catalog_) {
    catalog_ = catalog::Catalog::GetInstance();
    catalog_->Bootstrap();
  }

  if (nullptr == txn_manager_) {
    txn_manager_ = &concurrency::TransactionManagerFactory::GetInstance();
  }

  auto txn = txn_manager_->BeginTransaction();

  const auto db_obj = catalog_->GetDatabaseObject(database_name_, txn);
  const auto db_oid = db_obj->GetDatabaseOid();
  const auto table_objs = db_obj->GetTableObjects();

  // Scan tables to populate the internal maps
  for (const auto &table_obj : table_objs) {
    const auto table_oid = table_obj.first;

    table_id_map_[table_oid] = {};
    id_table_map_[table_oid] = {};
    auto &col_id_map = table_id_map_[table_oid];
    auto &id_col_map = id_table_map_[table_oid];

    const auto col_objs = table_obj.second->GetColumnObjects();
    size_t next_id = 0;
    for (const auto &col_obj : col_objs) {
      const auto col_oid = col_obj.first;
      col_id_map[col_oid] = next_id;
      id_col_map[next_id] = col_oid;
      next_id++;
    }

    table_offset_map_[table_oid] = next_table_offset_;
    table_offset_reverse_map_[next_table_offset_] = table_oid;
    next_table_offset_ += ((size_t)1U << next_id);
  }

  cur_index_config_ = std::unique_ptr<boost::dynamic_bitset<>>(
      new boost::dynamic_bitset<>(next_table_offset_));

  // Scan tables to populate current config
  for (const auto &table_obj : table_objs) {
    const auto table_oid = table_obj.first;
    const auto index_objs = table_obj.second->GetIndexObjects();
    for (const auto &index_obj : index_objs) {
      const auto &indexed_cols = index_obj.second->GetKeyAttrs();
      std::vector<oid_t> col_oids(indexed_cols);
      auto idx_obj =
          std::make_shared<brain::IndexObject>(db_oid, table_oid, col_oids);
      AddIndex(cur_index_config_, idx_obj);
    }
  }

  txn_manager_->CommitTransaction(txn);
}

size_t CompressedIndexConfiguration::GetLocalOffset(
    const oid_t table_oid, const std::set<oid_t> &column_oids) const {
  std::set<size_t> col_ids;
  const auto &col_id_map = table_id_map_.at(table_oid);
  for (const auto col_oid : column_oids) {
    size_t id = col_id_map.at(col_oid);
    col_ids.insert(id);
  }

  size_t final_offset = 0;

  for (const auto id : col_ids) {
    size_t offset = (((size_t)1U) << id);
    final_offset += offset;
  }

  return final_offset;
}

size_t CompressedIndexConfiguration::GetGlobalOffset(
    const std::shared_ptr<brain::IndexObject> &index_obj) const {
  oid_t table_oid = index_obj->table_oid;
  const auto local_offset = GetLocalOffset(table_oid, index_obj->column_oids);
  const auto table_offset = table_offset_map_.at(table_oid);
  return table_offset + local_offset;
}

bool CompressedIndexConfiguration::IsSet(
    const std::shared_ptr<brain::IndexObject> &index_obj) const {
  size_t offset = GetGlobalOffset(index_obj);
  return cur_index_config_->test(offset);
}

std::shared_ptr<brain::IndexObject> CompressedIndexConfiguration::GetIndex(
    size_t global_offset) const {
  size_t table_offset;
  auto it = table_offset_reverse_map_.lower_bound(global_offset);
  if (it == table_offset_reverse_map_.end()) {
    table_offset = table_offset_reverse_map_.rbegin()->first;
  } else {
    --it;
    table_offset = it->first;
  }

  auto local_offset = global_offset - table_offset;
  const oid_t table_oid = table_offset_reverse_map_.at(table_offset);
  const auto &id_col_map = id_table_map_.at(table_oid);
  std::vector<oid_t> col_oids;

  size_t cur_offset = 0;
  while (local_offset) {
    if (local_offset & (size_t)1U) {
      col_oids.push_back(id_col_map.at(cur_offset));
    }
    local_offset >>= 1;
    cur_offset += 1;
  }

  auto txn = txn_manager_->BeginTransaction();
  const auto db_oid =
      catalog_->GetDatabaseObject(database_name_, txn)->GetDatabaseOid();
  txn_manager_->CommitTransaction(txn);

  return std::make_shared<brain::IndexObject>(db_oid, table_oid, col_oids);
}

void CompressedIndexConfiguration::AddIndex(
    const std::shared_ptr<IndexObject> &idx_object) {
  size_t offset = GetGlobalOffset(idx_object);
  cur_index_config_->set(offset);
}

void CompressedIndexConfiguration::AddIndex(size_t offset) {
  cur_index_config_->set(offset);
}

void CompressedIndexConfiguration::AddIndex(
    std::unique_ptr<boost::dynamic_bitset<>> &bitmap,
    const std::shared_ptr<IndexObject> &idx_object) {
  size_t offset = GetGlobalOffset(idx_object);
  bitmap->set(offset);
}

void CompressedIndexConfiguration::AddIndex(
    std::unique_ptr<boost::dynamic_bitset<>> &bitmap, size_t offset) {
  bitmap->set(offset);
}

void CompressedIndexConfiguration::RemoveIndex(
    const std::shared_ptr<IndexObject> &idx_object) {
  size_t offset = GetGlobalOffset(idx_object);
  cur_index_config_->set(offset, false);
}

void CompressedIndexConfiguration::RemoveIndex(size_t offset) {
  cur_index_config_->set(offset, false);
}

std::unique_ptr<boost::dynamic_bitset<>>
CompressedIndexConfiguration::AddDropCandidate(
    const IndexConfiguration &indexes) {
  const auto &index_objs = indexes.GetIndexes();
  auto result = std::unique_ptr<boost::dynamic_bitset<>>(
      new boost::dynamic_bitset<>(next_table_offset_));

  // TODO: should we make db_oid, table_oid as private member?
  auto txn = txn_manager_->BeginTransaction();
  const auto db_oid =
      catalog_->GetDatabaseObject(database_name_, txn)->GetDatabaseOid();
  txn_manager_->CommitTransaction(txn);

  for (const auto &idx_obj : index_objs) {
    const auto table_oid = idx_obj->table_oid;
    const auto &column_oids = idx_obj->column_oids;
    const auto table_offset = table_offset_map_.at(table_oid);

    // Insert empty index
    AddIndex(result, table_offset);

    std::vector<oid_t> col_oids;
    for (const auto column_oid : column_oids) {
      col_oids.push_back(column_oid);

      // Insert prefix index
      auto idx_new =
          std::make_shared<brain::IndexObject>(db_oid, table_oid, col_oids);
      AddIndex(result, idx_new);
    }
  }

  return result;
}

std::unique_ptr<boost::dynamic_bitset<>>
CompressedIndexConfiguration::AddDropCandidate(
    std::unique_ptr<parser::SQLStatementList> sql_stmt_list) {
  if (nullptr == sql_stmt_list) {
    return std::unique_ptr<boost::dynamic_bitset<>>(
        new boost::dynamic_bitset<>(8));
  }
  return std::unique_ptr<boost::dynamic_bitset<>>(
      new boost::dynamic_bitset<>(16));
}

size_t CompressedIndexConfiguration::GetConfigurationCount() {
  return next_table_offset_;
}

const boost::dynamic_bitset<>
    *CompressedIndexConfiguration::GetCurrentIndexConfig() {
  return cur_index_config_.get();
}
}
}