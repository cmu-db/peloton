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
    const std::string &database_name, const std::set<oid_t> &ignore_table_oids,
    size_t max_index_size, catalog::Catalog *catalog,
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

  // Uniq identifier per index config
  size_t next_index_id = 0;
  // Scan tables to populate the internal maps
  for (const auto &table_obj : table_objs) {
    const auto table_oid = table_obj.first;

    if (ignore_table_oids.find(table_oid) != ignore_table_oids.end()) {
      continue;
    }

    // Enumerate configurations and prepare data structures for future usage
    table_indexid_map_[table_oid] = {};
    indexid_table_map_[table_oid] = {};
    auto &indexconf_id_map = table_indexid_map_[table_oid];
    auto &id_indexconf_map = indexid_table_map_[table_oid];
    const auto col_objs = table_obj.second->GetColumnObjects();
    std::vector<oid_t> null_conf;
    std::vector<oid_t> cols;
    for (const auto &col_obj : col_objs) {
      cols.push_back(col_obj.first);
    }
    EnumerateConfigurations(cols, max_index_size, indexconf_id_map,
                            id_indexconf_map, null_conf, next_index_id);

    table_offset_map_[table_oid] = next_table_offset_;
    table_offset_reverse_map_[next_table_offset_] = table_oid;
    next_table_offset_ += indexconf_id_map.size();
  }

  cur_index_config_ = std::unique_ptr<boost::dynamic_bitset<>>(
      new boost::dynamic_bitset<>(next_table_offset_));

  // Scan tables to populate current config
  for (const auto &table_obj : table_objs) {
    const auto table_oid = table_obj.first;

    if (ignore_table_oids.find(table_oid) != ignore_table_oids.end()) {
      continue;
    }

    const auto index_objs = table_obj.second->GetIndexObjects();
    if (index_objs.empty()) {
      SetBit(table_offset_map_.at(table_oid));
    } else {
      for (const auto &index_obj : index_objs) {
        const auto &indexed_cols = index_obj.second->GetKeyAttrs();
        const auto index_oid = index_obj.first;

        std::vector<oid_t> col_oids(indexed_cols);
        auto idx_obj = std::make_shared<brain::HypotheticalIndexObject>(
            db_oid, table_oid, col_oids);

        const auto global_index_offset = GetGlobalOffset(idx_obj);
        offset_to_indexoid_[global_index_offset] = index_oid;

        SetBit(global_index_offset);
      }
    }
  }

  txn_manager_->CommitTransaction(txn);
}

void CompressedIndexConfigContainer::EnumerateConfigurations(
    const std::vector<peloton::oid_t> &cols, size_t max_index_size,
    std::map<std::vector<peloton::oid_t>, size_t> &indexconf_id_map,
    std::map<size_t, std::vector<peloton::oid_t>> &id_indexconf_map,
    std::vector<peloton::oid_t> &index_conf, size_t &next_id) {
  if (index_conf.size() <= std::min<size_t>(max_index_size, cols.size())) {
    indexconf_id_map[index_conf] = next_id;
    id_indexconf_map[next_id] = index_conf;
    next_id++;
  }
  for (auto col : cols) {
    if (std::find(index_conf.begin(), index_conf.end(), col) ==
        index_conf.end()) {
      index_conf.push_back(col);
      EnumerateConfigurations(cols, max_index_size, indexconf_id_map,
                              id_indexconf_map, index_conf, next_id);
      index_conf.pop_back();
    }
  }
}

void CompressedIndexConfigContainer::AdjustIndexes(
    const boost::dynamic_bitset<> &new_bitset) {
  boost::dynamic_bitset<> &ori_bitset = *cur_index_config_;

  const auto drop_bitset = ori_bitset - new_bitset;

  auto txn = txn_manager_->BeginTransaction();
  const auto database_oid =
      catalog_->GetDatabaseObject(database_name_, txn)->GetDatabaseOid();
  for (size_t current_bit = drop_bitset.find_first();
       current_bit != boost::dynamic_bitset<>::npos;
       current_bit = drop_bitset.find_next(current_bit)) {
    // 1. unset current bit
    UnsetBit(current_bit);

    // Current bit is not an empty index (empty set)
    if (table_offset_reverse_map_.find(current_bit) ==
        table_offset_reverse_map_.end()) {
      // 2. drop its corresponding index in catalog
      oid_t index_oid = offset_to_indexoid_.at(current_bit);
      // TODO (weichenl): This will call into the storage manager and delete the
      // index in the real table storage, which we don't have on the brain side.
      // We need a way to only delete the entry in the catalog table, and then
      // issue a RPC call to let Peloton server really drop the index (using
      // this DropIndex method).
      catalog_->DropIndex(database_oid, index_oid, txn);

      // 3. erase its entry in the maps
      offset_to_indexoid_.erase(current_bit);
    }
  }
  txn_manager_->CommitTransaction(txn);

  const auto add_bitset = new_bitset - ori_bitset;

  for (size_t current_bit = add_bitset.find_first();
       current_bit != boost::dynamic_bitset<>::npos;
       current_bit = drop_bitset.find_next(current_bit)) {
    // 1. set current bit
    SetBit(current_bit);

    // Current bit is not an empty index (empty set)
    if (table_offset_reverse_map_.find(current_bit) ==
        table_offset_reverse_map_.end()) {
      txn = txn_manager_->BeginTransaction();

      // 2. add its corresponding index in catalog
      const auto new_index = GetIndex(current_bit);
      const auto table_name = catalog_->GetDatabaseObject(database_name_, txn)
                                  ->GetTableObject(new_index->table_oid)
                                  ->GetTableName();

      std::set<oid_t> temp_oids(new_index->column_oids.begin(),
                                new_index->column_oids.end());

      std::vector<oid_t> index_vector(temp_oids.begin(), temp_oids.end());

      std::ostringstream stringStream;
      stringStream << "automated_index_" << current_bit;
      const std::string temp_index_name = stringStream.str();

      catalog_->CreateIndex(database_name_, DEFUALT_SCHEMA_NAME, table_name,
                            index_vector, temp_index_name, false,
                            IndexType::BWTREE, txn);

      txn_manager_->CommitTransaction(txn);

      txn = txn_manager_->BeginTransaction();

      // 3. insert its entry in the maps
      const auto index_object = catalog_->GetDatabaseObject(database_name_, txn)
                                    ->GetTableObject(new_index->table_oid)
                                    ->GetIndexObject(temp_index_name);
      const auto index_oid = index_object->GetIndexOid();

      txn_manager_->CommitTransaction(txn);

      offset_to_indexoid_[current_bit] = index_oid;
    }
  }
}

//**Setter fns**/
void CompressedIndexConfigContainer::SetBit(
    const std::shared_ptr<HypotheticalIndexObject> &idx_object) {
  size_t offset = GetGlobalOffset(idx_object);
  cur_index_config_->set(offset);
}

void CompressedIndexConfigContainer::SetBit(size_t offset) {
  cur_index_config_->set(offset);
}

void CompressedIndexConfigContainer::UnsetBit(
    const std::shared_ptr<HypotheticalIndexObject> &idx_object) {
  size_t offset = GetGlobalOffset(idx_object);
  cur_index_config_->set(offset, false);
}

void CompressedIndexConfigContainer::UnsetBit(size_t offset) {
  cur_index_config_->set(offset, false);
}

//**Getter fns**/

size_t CompressedIndexConfigContainer::GetGlobalOffset(
    const std::shared_ptr<brain::HypotheticalIndexObject> &index_obj) const {
  oid_t table_oid = index_obj->table_oid;
  return table_indexid_map_.at(table_oid).at(index_obj->column_oids);
}

bool CompressedIndexConfigContainer::IsSet(
    const std::shared_ptr<brain::HypotheticalIndexObject> &index_obj) const {
  size_t offset = GetGlobalOffset(index_obj);
  return cur_index_config_->test(offset);
}

bool CompressedIndexConfigContainer::IsSet(const size_t offset) const {
  return cur_index_config_->test(offset);
}

std::shared_ptr<brain::HypotheticalIndexObject>
CompressedIndexConfigContainer::GetIndex(size_t global_offset) const {
  size_t table_offset;
  auto it = table_offset_reverse_map_.lower_bound(global_offset);
  if (it == table_offset_reverse_map_.end()) {
    table_offset = table_offset_reverse_map_.rbegin()->first;
  } else {
    --it;
    table_offset = it->first;
  }

  const oid_t table_oid = table_offset_reverse_map_.at(table_offset);
  std::vector<oid_t> col_oids =
      indexid_table_map_.at(table_oid).at(global_offset);

  auto txn = txn_manager_->BeginTransaction();
  const auto db_oid =
      catalog_->GetDatabaseObject(database_name_, txn)->GetDatabaseOid();
  txn_manager_->CommitTransaction(txn);

  return std::make_shared<brain::HypotheticalIndexObject>(db_oid, table_oid,
                                                          col_oids);
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

size_t CompressedIndexConfigContainer::GetTableOffsetStart(
    oid_t table_oid) const {
  return table_offset_map_.at(table_oid);
}

size_t CompressedIndexConfigContainer::GetTableOffsetEnd(
    oid_t table_oid) const {
  size_t start_idx = GetTableOffsetStart(table_oid);
  return GetNextTableIdx(start_idx);
}

size_t CompressedIndexConfigContainer::GetNextTableIdx(size_t start_idx) const {
  auto next_tbl_offset_iter = table_offset_reverse_map_.upper_bound(start_idx);
  if (next_tbl_offset_iter == table_offset_reverse_map_.end()) {
    return GetConfigurationCount();
  } else {
    return next_tbl_offset_iter->first;
  }
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
    size_t start_idx = tbl_offset_iter->first;
    size_t end_idx = GetNextTableIdx(start_idx);
    oid_t table_oid = tbl_offset_iter->second;
    str_stream << "Table OID: " << table_oid << " Compressed Section: "
               << bitset_str.substr(start_idx, end_idx - start_idx)
               << std::endl;
    for (auto col_iter : table_indexid_map_.at(table_oid)) {
      str_stream << "(";
      for (auto col_oid : col_iter.first) {
        str_stream << col_oid << ",";
      }
      str_stream << "):" << col_iter.second << std::endl;
    }
  }
  return str_stream.str();
}

size_t CompressedIndexConfigContainer::GetNumIndexes(oid_t table_oid) const {
  size_t start_idx = GetTableOffsetStart(table_oid);
  size_t end_idx = GetNextTableIdx(start_idx);
  if (IsSet(start_idx)) {
    return 0;
  } else {
    size_t idx = GetNextSetIndexConfig(start_idx);
    size_t count = 0;
    while (idx != boost::dynamic_bitset<>::npos && idx < end_idx) {
      count += 1;
      idx = GetNextSetIndexConfig(idx);
    }
    return count;
  }
}

size_t CompressedIndexConfigContainer::GetNextSetIndexConfig(
    size_t from_idx) const {
  return cur_index_config_->find_next(from_idx);
}

bool CompressedIndexConfigContainer::EmptyConfig(
    peloton::oid_t table_oid) const {
  size_t table_offset = table_offset_map_.at(table_oid);
  return IsSet(table_offset);
}

}  // namespace brain
}  // namespace peloton