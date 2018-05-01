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
      AddIndex(idx_obj);
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

bool CompressedIndexConfiguration::IsSet(const size_t offset) const {
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
    boost::dynamic_bitset<> &bitmap,
    const std::shared_ptr<IndexObject> &idx_object) {
  size_t offset = GetGlobalOffset(idx_object);
  bitmap.set(offset);
}

void CompressedIndexConfiguration::AddIndex(boost::dynamic_bitset<> &bitmap,
                                            size_t offset) {
  bitmap.set(offset);
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
CompressedIndexConfiguration::AddCandidates(const std::string& query) {

  auto result = std::unique_ptr<boost::dynamic_bitset<>>(
      new boost::dynamic_bitset<>(next_table_offset_));

  auto txn = txn_manager_->BeginTransaction();
  catalog_->GetDatabaseObject(database_name_, txn);
  std::vector<planner::col_triplet> affected_cols_vector =
      planner::PlanUtil::GetIndexableColumns(
          txn->catalog_cache,
          ToBindedSqlStmtList(query),
          database_name_);
  txn_manager_->CommitTransaction(txn);

  // Aggregate all columns in the same table
  std::unordered_map<oid_t, brain::IndexObject> aggregate_map;

  for (const auto &each_triplet : affected_cols_vector) {
    const auto db_oid = std::get<0>(each_triplet);
    const auto table_oid = std::get<1>(each_triplet);
    const auto col_oid = std::get<2>(each_triplet);

    if (aggregate_map.find(table_oid) == aggregate_map.end()) {
      aggregate_map[table_oid] = brain::IndexObject();
      aggregate_map.at(table_oid).db_oid = db_oid;
      aggregate_map.at(table_oid).table_oid = table_oid;
    }

    aggregate_map.at(table_oid).column_oids.insert(col_oid);
  }

  const auto db_oid = aggregate_map.begin()->second.db_oid;

  for (const auto it : aggregate_map) {
    const auto table_oid = it.first;
    const auto &column_oids = it.second.column_oids;
    const auto table_offset = table_offset_map_.at(table_oid);

    // Insert empty index
    AddIndex(*result, table_offset);

    // For each index, iterate through its columns
    // and incrementally add the columns to the prefix closure of current table
    std::vector<oid_t> col_oids;
    for (const auto column_oid : column_oids) {
      col_oids.push_back(column_oid);

      // Insert prefix index
      auto idx_new =
          std::make_shared<brain::IndexObject>(db_oid, table_oid, col_oids);
      AddIndex(*result, idx_new);
    }
  }

  return result;
}

std::shared_ptr<brain::IndexObject>
CompressedIndexConfiguration::ConvertIndexTriplet(
    const planner::col_triplet &idx_triplet) {
  const auto db_oid = std::get<0>(idx_triplet);
  const auto table_oid = std::get<1>(idx_triplet);
  const auto idx_oid = std::get<2>(idx_triplet);

  auto txn = txn_manager_->BeginTransaction();
  const auto db_obj = catalog_->GetDatabaseObject(db_oid, txn);
  const auto table_obj = db_obj->GetTableObject(table_oid);
  const auto idx_obj = table_obj->GetIndexObject(idx_oid);
  const auto col_oids = idx_obj->GetKeyAttrs();
  std::vector<oid_t> input_oids(col_oids);

  txn_manager_->CommitTransaction(txn);

  return std::make_shared<brain::IndexObject>(db_oid, table_oid, input_oids);
}

std::unique_ptr<parser::SQLStatementList>
    CompressedIndexConfiguration::ToBindedSqlStmtList(const std::string &query_string) {
  auto txn = txn_manager_->BeginTransaction();
  auto &peloton_parser = parser::PostgresParser::GetInstance();
  auto sql_stmt_list = peloton_parser.BuildParseTree(query_string);
  auto sql_stmt = sql_stmt_list->GetStatement(0);
  auto bind_node_visitor = binder::BindNodeVisitor(txn, database_name_);
  bind_node_visitor.BindNameToNode(sql_stmt);
  txn_manager_->CommitTransaction(txn);

  return sql_stmt_list;
}

std::unique_ptr<boost::dynamic_bitset<>>
CompressedIndexConfiguration::DropCandidates(const std::string& query) {
  auto result = std::unique_ptr<boost::dynamic_bitset<>>(
      new boost::dynamic_bitset<>(next_table_offset_));

  auto sql_stmt_list = ToBindedSqlStmtList(query);
  auto sql_stmt = sql_stmt_list->GetStatement(0);

  auto txn = txn_manager_->BeginTransaction();
  catalog_->GetDatabaseObject(database_name_, txn);
  std::vector<planner::col_triplet> affected_indexes =
      planner::PlanUtil::GetAffectedIndexes(txn->catalog_cache, *sql_stmt);
  for (const auto &col_triplet : affected_indexes) {
    auto idx_obj = ConvertIndexTriplet(col_triplet);
    AddIndex(*result, idx_obj);
  }
  txn_manager_->CommitTransaction(txn);
  return result;
}

size_t CompressedIndexConfiguration::GetConfigurationCount() const {
  return next_table_offset_;
}

const boost::dynamic_bitset<>
    *CompressedIndexConfiguration::GetCurrentIndexConfig() const {
  return cur_index_config_.get();
}

void CompressedIndexConfiguration::ToEigen(vector_eig &curr_config_vec) const {
  // Note that the representation is reversed - but this should not affect
  // anything
  curr_config_vec = vector_eig::Zero(GetConfigurationCount());
  size_t config_id = cur_index_config_->find_first();
  while (config_id != boost::dynamic_bitset<>::npos) {
    curr_config_vec[config_id] = 1.0;
    config_id = cur_index_config_->find_next(config_id);
  }
}

std::string CompressedIndexConfiguration::ToString() const {
  // First get the entire bitset
  std::stringstream str_stream;
  std::string bitset_str;
  boost::to_string(*GetCurrentIndexConfig(), bitset_str);
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
}
}