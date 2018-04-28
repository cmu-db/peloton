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
    catalog::Catalog *cat, concurrency::TransactionManager *txn_manager)
    : catalog_{cat}, txn_manager_{txn_manager}, next_table_offset_{0} {
  catalog_->Bootstrap();
}

// Create a new database
void CompressedIndexConfiguration::CreateDatabase(const std::string &db_name) {
  database_name_ = db_name;

  auto txn = txn_manager_->BeginTransaction();
  catalog_->CreateDatabase(database_name_, txn);
  txn_manager_->CommitTransaction(txn);
}

// Create a new table with schema (a INT, b INT, c INT).
void CompressedIndexConfiguration::CreateTable(const std::string &table_name) {
  auto a_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "a", true);
  auto b_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "b", true);
  auto c_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "c", true);
  std::unique_ptr<catalog::Schema> table_schema(
      new catalog::Schema({a_column, b_column, c_column}));

  auto txn = txn_manager_->BeginTransaction();
  catalog_->CreateTable(database_name_, table_name, std::move(table_schema),
                        txn);
  txn_manager_->CommitTransaction(txn);

  std::vector<oid_t> col_oids;
  txn = txn_manager_->BeginTransaction();
  const auto table_obj =
      catalog_->GetTableObject(database_name_, table_name, txn);
  const oid_t table_oid = table_obj->GetTableOid();
  const auto col_objs = table_obj->GetColumnObjects();
  for (const auto &col_it : col_objs) {
    col_oids.push_back(col_it.first);
  }
  txn_manager_->CommitTransaction(txn);

  table_id_map_[table_oid] = {};
  id_table_map_[table_oid] = {};
  auto &col_id_map = table_id_map_[table_oid];
  auto &id_col_map = id_table_map_[table_oid];

  size_t next_id = 0;
  for (const auto col_oid : col_oids) {
    col_id_map[col_oid] = next_id;
    id_col_map[next_id] = col_oid;
    next_id++;
  }

  table_offset_map_[table_oid] = next_table_offset_;
  next_table_offset_ += ((size_t)1 << col_oids.size());
}

void CompressedIndexConfiguration::CreateIndex_A(
    const std::string &table_name) {
  // create index on (a, b) and (b, c)
  // (a, b) -> 110 -> 6
  // (b, c) -> 011 -> 3
  auto txn = txn_manager_->BeginTransaction();
  const auto db_obj = catalog_->GetDatabaseWithName(database_name_, txn);
  const auto table_obj = db_obj->GetTableWithName(table_name);

  auto col_a = table_obj->GetSchema()->GetColumnID("a");
  auto col_b = table_obj->GetSchema()->GetColumnID("b");
  auto col_c = table_obj->GetSchema()->GetColumnID("c");
  std::vector<oid_t> index_a_b = {col_a, col_b};
  std::vector<oid_t> index_b_c = {col_b, col_c};

  catalog_->CreateIndex(database_name_, table_name, index_a_b, "index_a_b",
                        false, IndexType::BWTREE, txn);
  catalog_->CreateIndex(database_name_, table_name, index_b_c, "index_b_c",
                        false, IndexType::BWTREE, txn);

  txn_manager_->CommitTransaction(txn);
}

void CompressedIndexConfiguration::CreateIndex_B(
    const std::string &table_name) {
  // create index on (a, c)
  // (a, c) -> 101 -> 5
  auto txn = txn_manager_->BeginTransaction();
  const auto db_obj = catalog_->GetDatabaseWithName(database_name_, txn);
  const auto table_obj = db_obj->GetTableWithName(table_name);

  auto col_a = table_obj->GetSchema()->GetColumnID("a");
  auto col_c = table_obj->GetSchema()->GetColumnID("c");
  std::vector<oid_t> index_a_c = {col_a, col_c};

  catalog_->CreateIndex(database_name_, table_name, index_a_c, "index_a_c",
                        false, IndexType::BWTREE, txn);

  txn_manager_->CommitTransaction(txn);
}

void CompressedIndexConfiguration::DropTable(const std::string &table_name) {
  auto txn = txn_manager_->BeginTransaction();
  catalog_->DropTable(database_name_, table_name, txn);
  txn_manager_->CommitTransaction(txn);
}

void CompressedIndexConfiguration::DropDatabase() {
  auto txn = txn_manager_->BeginTransaction();
  catalog_->DropDatabaseWithName(database_name_, txn);
  txn_manager_->CommitTransaction(txn);
}

size_t CompressedIndexConfiguration::GetLocalOffset(
    const oid_t table_oid, const std::set<oid_t> &column_oids) {
  std::set<size_t> offsets;
  const auto &col_id_map = table_id_map_[table_oid];
  for (const auto col_oid : column_oids) {
    size_t offset = col_id_map.find(col_oid)->second;
    offsets.insert(offset);
  }

  size_t map_size = col_id_map.size();
  size_t final_offset = 0;
  size_t step = (((size_t)1U) << map_size) >> 1;
  for (size_t i = 0; i < map_size; ++i) {
    if (offsets.find(i) != offsets.end()) {
      final_offset += step;
    }
    step >>= 1;
  }

  return final_offset;
}

size_t CompressedIndexConfiguration::GetGlobalOffset(
    const std::shared_ptr<brain::IndexObject> &index_obj) {
  oid_t table_oid = index_obj->table_oid;
  const auto local_offset = GetLocalOffset(table_oid, index_obj->column_oids);
  const auto table_offset = table_offset_map_.find(table_oid)->second;
  return table_offset + local_offset;
}

bool CompressedIndexConfiguration::IsSet(
    const std::shared_ptr<boost::dynamic_bitset<>> &bitset,
    const std::shared_ptr<brain::IndexObject> &index_obj) {
  size_t offset = GetGlobalOffset(index_obj);
  return bitset->test(offset);
}

void CompressedIndexConfiguration::Set(
    const std::shared_ptr<boost::dynamic_bitset<>> &bitset,
    const std::shared_ptr<brain::IndexObject> &index_obj) {
  size_t offset = GetGlobalOffset(index_obj);
  bitset->set(offset);
}

std::shared_ptr<boost::dynamic_bitset<>>
CompressedIndexConfiguration::GenerateCurrentBitSet() {
  auto result = std::make_shared<boost::dynamic_bitset<>>(next_table_offset_);

  auto txn = txn_manager_->BeginTransaction();

  const auto db_obj = catalog_->GetDatabaseObject(database_name_, txn);
  const auto db_oid = db_obj->GetDatabaseOid();
  const auto table_objs = db_obj->GetTableObjects();
  for (const auto &table_obj : table_objs) {
    const auto table_oid = table_obj.first;
    const auto index_objs = table_obj.second->GetIndexObjects();
    for (const auto &index_obj : index_objs) {
      const auto &indexed_cols = index_obj.second->GetKeyAttrs();
      std::vector<oid_t> col_oids(indexed_cols);
      auto idx_obj =
          std::make_shared<brain::IndexObject>(db_oid, table_oid, col_oids);
      Set(result, idx_obj);
    }
  }

  txn_manager_->CommitTransaction(txn);

  return result;
}

void CompressedIndexConfiguration::AddIndex(
    std::shared_ptr<boost::dynamic_bitset<>> &bitset,
    const std::shared_ptr<IndexObject> &idx_object) {
  size_t offset = GetGlobalOffset(idx_object);
  bitset->set(offset);
}

void CompressedIndexConfiguration::AddIndex(
    std::shared_ptr<boost::dynamic_bitset<>> &bitset, size_t offset) {
  bitset->set(offset);
}

void CompressedIndexConfiguration::RemoveIndex(
    std::shared_ptr<boost::dynamic_bitset<>> &bitset,
    const std::shared_ptr<IndexObject> &idx_object) {
  size_t offset = GetGlobalOffset(idx_object);
  bitset->set(offset, false);
}

void CompressedIndexConfiguration::RemoveIndex(
    std::shared_ptr<boost::dynamic_bitset<>> &bitset, size_t offset) {
  bitset->set(offset, false);
}

int CompressedIndexConfiguration::GetConfigurationCount() {
  return 10;
}

std::shared_ptr<boost::dynamic_bitset<>>
CompressedIndexConfiguration::AddCandidate(const IndexConfiguration &indexes) {
  const auto index_objs = indexes.GetIndexes();
  auto result = std::make_shared<boost::dynamic_bitset<>>(next_table_offset_);

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

std::shared_ptr<boost::dynamic_bitset<>>
CompressedIndexConfiguration::DropCandidate(const IndexConfiguration &indexes) {
  int a = 8;
  if (0 == indexes.GetIndexCount()) {
    a = 16;
  }
  return std::make_shared<boost::dynamic_bitset<>>(a);
}
}
}