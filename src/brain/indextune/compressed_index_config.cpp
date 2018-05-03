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

CompressedIndexConfigContainer::CompressedIndexConfigContainer(
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
    if (index_objs.empty()) {
      SetBit(table_offset_map_.at(table_oid));
    } else {
      for (const auto &index_obj : index_objs) {
        const auto &indexed_cols = index_obj.second->GetKeyAttrs();
        const auto index_oid = index_obj.first;

        std::vector<oid_t> col_oids(indexed_cols);
        auto idx_obj =
            std::make_shared<brain::IndexObject>(db_oid, table_oid, col_oids);

        const auto global_index_offset = GetGlobalOffset(idx_obj);
        index_id_map_[index_oid] = global_index_offset;
        index_id_reverse_map_[global_index_offset] = index_oid;

        SetBit(global_index_offset);
      }
    }
  }

  txn_manager_->CommitTransaction(txn);
}

size_t CompressedIndexConfigContainer::GetLocalOffset(
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

size_t CompressedIndexConfigContainer::GetGlobalOffset(
    const std::shared_ptr<brain::IndexObject> &index_obj) const {
  oid_t table_oid = index_obj->table_oid;
  const auto local_offset = GetLocalOffset(table_oid, index_obj->column_oids);
  const auto table_offset = table_offset_map_.at(table_oid);
  return table_offset + local_offset;
}

bool CompressedIndexConfigContainer::IsSet(
    const std::shared_ptr<brain::IndexObject> &index_obj) const {
  size_t offset = GetGlobalOffset(index_obj);
  return cur_index_config_->test(offset);
}

bool CompressedIndexConfigContainer::IsSet(const size_t offset) const {
  return cur_index_config_->test(offset);
}

std::shared_ptr<brain::IndexObject> CompressedIndexConfigContainer::GetIndex(
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

void CompressedIndexConfigContainer::SetBit(
    const std::shared_ptr<IndexObject> &idx_object) {
  size_t offset = GetGlobalOffset(idx_object);
  cur_index_config_->set(offset);
}

void CompressedIndexConfigContainer::SetBit(size_t offset) {
  cur_index_config_->set(offset);
}

void CompressedIndexConfigContainer::UnsetBit(
    const std::shared_ptr<IndexObject> &idx_object) {
  size_t offset = GetGlobalOffset(idx_object);
  cur_index_config_->set(offset, false);
}

void CompressedIndexConfigContainer::UnsetBit(size_t offset) {
  cur_index_config_->set(offset, false);
}

size_t CompressedIndexConfigContainer::GetConfigurationCount() const {
  return next_table_offset_;
}

const boost::dynamic_bitset<>
    *CompressedIndexConfigContainer::GetCurrentIndexConfig() const {
  return cur_index_config_.get();
}

concurrency::TransactionManager *
CompressedIndexConfigContainer::GetTransactionManager() {
  return txn_manager_;
}

catalog::Catalog *CompressedIndexConfigContainer::GetCatalog() {
  return catalog_;
}

std::string CompressedIndexConfigContainer::GetDatabaseName() const {
  return database_name_;
}

size_t CompressedIndexConfigContainer::GetTableOffset(oid_t table_oid) const {
  return table_offset_map_.at(table_oid);
}

std::string CompressedIndexConfigContainer::ToString() const {
  // First get the entire bitset
  std::stringstream str_stream;
  std::string bitset_str;
  boost::to_string(*GetCurrentIndexConfig(), bitset_str);
  // since bitset follows MSB <---- LSB
  std::reverse(bitset_str.begin(), bitset_str.end());
  str_stream << "Database: " << database_name_ << std::endl;
  str_stream << "Compressed Index Representation: " << bitset_str << std::endl;
  for (auto tbl_offset_iter = table_offset_reverse_map_.begin();
       tbl_offset_iter != table_offset_reverse_map_.end(); ++tbl_offset_iter) {
    auto next_tbl_offset_iter = std::next(tbl_offset_iter);
    size_t start_idx = tbl_offset_iter->first;
    size_t end_idx;
    if (next_tbl_offset_iter == table_offset_reverse_map_.end()) {
      end_idx = GetConfigurationCount();
    } else {
      end_idx = next_tbl_offset_iter->first;
    }
    oid_t table_oid = tbl_offset_iter->second;
    str_stream << "Table OID: " << table_oid << " Compressed Section: "
               << bitset_str.substr(start_idx, end_idx) << std::endl;
    for (auto col_iter = table_id_map_.at(table_oid).begin();
         col_iter != table_id_map_.at(table_oid).end(); col_iter++) {
      str_stream << "Col OID: " << col_iter->first
                 << " Offset: " << col_iter->second << std::endl;
    }
  }

  return str_stream.str();
}

void CompressedIndexConfigContainer::ToEigen(
    const boost::dynamic_bitset<> &config_set, vector_eig &config_vec) const {
  // Note that the representation is reversed - but this should not affect
  // anything
  PELOTON_ASSERT(config_set.size() == GetConfigurationCount());
  config_vec = vector_eig::Zero(config_set.size());
  size_t config_id = config_set.find_first();
  while (config_id != boost::dynamic_bitset<>::npos) {
    config_vec[config_id] = 1.0;
    config_id = config_set.find_next(config_id);
  }
}

void CompressedIndexConfigContainer::ToEigen(vector_eig &config_vec) const {
  // Note that the representation is reversed - but this should not affect
  // anything
  ToEigen(*cur_index_config_, config_vec);
}

void CompressedIndexConfigContainer::ToCoveredEigen(
    vector_eig &config_vec) const {
  ToCoveredEigen(*cur_index_config_, config_vec);
}

void CompressedIndexConfigContainer::ToCoveredEigen(
    const boost::dynamic_bitset<> &config_set, vector_eig &config_vec) const {
  // Note that the representation is reversed - but this should not affect
  // anything
  PELOTON_ASSERT(GetConfigurationCount() == config_set.size());
  config_vec = vector_eig::Zero(GetConfigurationCount());
  for (auto tbl_offset_iter = table_offset_reverse_map_.begin();
       tbl_offset_iter != table_offset_reverse_map_.end(); ++tbl_offset_iter) {
    auto next_tbl_offset_iter = std::next(tbl_offset_iter);
    size_t start_idx = tbl_offset_iter->first;
    size_t end_idx;
    if (next_tbl_offset_iter == table_offset_reverse_map_.end()) {
      end_idx = GetConfigurationCount();
    } else {
      end_idx = next_tbl_offset_iter->first;
    }
    size_t last_set_idx = start_idx;
    while (last_set_idx < end_idx) {
      size_t next_set_idx = config_set.find_next(last_set_idx);
      if (next_set_idx >= end_idx) break;
      last_set_idx = next_set_idx;
    }
    config_vec.segment(start_idx, last_set_idx - start_idx + 1).array() = 1.0;
  }
}

void CompressedIndexConfigContainer::AdjustIndexes(
    const boost::dynamic_bitset<> &new_bitset) {
  boost::dynamic_bitset<> &ori_bitset = *cur_index_config_;

  const auto drop_bitset = ori_bitset - new_bitset;

  auto txn = txn_manager_->BeginTransaction();
  for (size_t current_bit = drop_bitset.find_first();
       current_bit != boost::dynamic_bitset<>::npos;
       current_bit = drop_bitset.find_next(current_bit)) {
    // 1. unset current bit
    UnsetBit(current_bit);

    // Current bit is not an empty index (empty set)
    if (table_offset_reverse_map_.find(current_bit) ==
        table_offset_reverse_map_.end()) {
      // 2. drop its corresponding index in catalog
      oid_t index_oid = index_id_reverse_map_.at(current_bit);
      catalog_->DropIndex(index_oid, txn);

      // 3. erase its entry in the maps
      index_id_reverse_map_.erase(current_bit);
      index_id_map_.erase(index_oid);
    }
  }
  txn_manager_->CommitTransaction(txn);

  const auto add_bitset = new_bitset - ori_bitset;

  txn = txn_manager_->BeginTransaction();

  const auto db_obj = catalog_->GetDatabaseObject(database_name_, txn);

  for (size_t current_bit = add_bitset.find_first();
       current_bit != boost::dynamic_bitset<>::npos;
       current_bit = drop_bitset.find_next(current_bit)) {
    // 1. set current bit
    SetBit(current_bit);

    // Current bit is not an empty index (empty set)
    if (table_offset_reverse_map_.find(current_bit) ==
        table_offset_reverse_map_.end()) {
      // 2. add its corresponding index in catalog
      const auto new_index = GetIndex(current_bit);
      const auto table_obj = db_obj->GetTableObject(new_index->table_oid);
      const auto table_name = table_obj->GetTableName();

      std::vector<oid_t> index_vector(new_index->column_oids.begin(),
                                      new_index->column_oids.end());

      std::ostringstream stringStream;
      stringStream << "automated_index_" << current_bit;
      const std::string temp_index_name = stringStream.str();

      catalog_->CreateIndex(database_name_, table_name, index_vector,
                            temp_index_name, false, IndexType::BWTREE, txn);

      // 3. insert its entry in the maps
      const auto index_object = table_obj->GetIndexObject(temp_index_name);
      const auto index_oid = index_object->GetIndexOid();

      index_id_map_[index_oid] = current_bit;
      index_id_reverse_map_[current_bit] = index_oid;
    }
  }

  txn_manager_->CommitTransaction(txn);
}
}
}