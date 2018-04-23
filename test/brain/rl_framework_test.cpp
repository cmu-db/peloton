//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rl_framework_test.cpp
//
// Identification: test/brain/rl_framework_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/index_selection.h"
#include "catalog/catalog.h"
#include "catalog/database_catalog.h"
#include "catalog/index_catalog.h"
#include "catalog/table_catalog.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "util/file_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// RL Framework Tests
//===--------------------------------------------------------------------===//

class RLFrameworkTest : public PelotonTest {
 private:
  std::string database_name_;
  catalog::Catalog *catalog_;
  concurrency::TransactionManager *txn_manager_;
  std::unordered_map<std::string, oid_t> column_id_map_;
  std::unordered_map<oid_t, std::string> id_column_map_;
  std::unordered_map<std::string, oid_t> config_id_map_;
  std::unordered_map<oid_t, std::string> id_config_map_;
  oid_t next_column_id_;
  oid_t next_config_id_;

 public:
  RLFrameworkTest()
      : catalog_{catalog::Catalog::GetInstance()},
        txn_manager_(&concurrency::TransactionManagerFactory::GetInstance()),
        next_column_id_(0),
        next_config_id_(0) {
    catalog_->Bootstrap();
  }

  // Create a new database
  void CreateDatabase(const std::string &db_name) {
    database_name_ = db_name;

    auto txn = txn_manager_->BeginTransaction();
    catalog_->CreateDatabase(database_name_, txn);
    txn_manager_->CommitTransaction(txn);
  }

  // Create a new table with schema (a INT, b INT, c INT).
  void CreateTable(const std::string &table_name) {
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
  }

  void DropTable(const std::string &table_name) {
    auto txn = txn_manager_->BeginTransaction();
    catalog_->DropTable(database_name_, table_name, txn);
    txn_manager_->CommitTransaction(txn);
  }

  void DropDatabase() {
    auto txn = txn_manager_->BeginTransaction();
    catalog_->DropDatabaseWithName(database_name_, txn);
    txn_manager_->CommitTransaction(txn);
  }

  std::vector<std::tuple<oid_t, oid_t, oid_t>> GetAllColumns() {
    std::vector<std::tuple<oid_t, oid_t, oid_t>> result;

    auto txn = txn_manager_->BeginTransaction();

    const auto db_object = catalog_->GetDatabaseObject(database_name_, txn);
    oid_t db_oid = db_object->GetDatabaseOid();
    const auto table_objects = db_object->GetTableObjects();

    for (const auto &it : table_objects) {
      oid_t table_oid = it.first;
      const auto table_obj = it.second;
      const auto column_objects = table_obj->GetColumnObjects();
      for (const auto &col_it : column_objects) {
        oid_t col_oid = col_it.first;
        result.emplace_back(db_oid, table_oid, col_oid);
      }
    }

    txn_manager_->CommitTransaction(txn);

    return result;
  }

  std::vector<std::tuple<oid_t, oid_t, oid_t>> GetAllIndexes() {
    std::vector<std::tuple<oid_t, oid_t, oid_t>> result;

    auto txn = txn_manager_->BeginTransaction();

    const auto db_object = catalog_->GetDatabaseObject(database_name_, txn);
    oid_t db_oid = db_object->GetDatabaseOid();
    const auto table_objects = db_object->GetTableObjects();

    for (const auto &it : table_objects) {
      oid_t table_oid = it.first;
      const auto table_obj = it.second;
      const auto index_objects = table_obj->GetIndexObjects();
      for (const auto &idx_it : index_objects) {
        oid_t idx_oid = idx_it.first;
        result.emplace_back(db_oid, table_oid, idx_oid);
      }
    }

    txn_manager_->CommitTransaction(txn);

    return result;
  }

  std::string GetStringFromTriplet(oid_t a, oid_t b, oid_t c) {
    std::ostringstream str_stream;
    str_stream << a << ":" << b << ":" << c;
    return str_stream.str();
  }

  std::tuple<oid_t, oid_t, oid_t> GetTripletFromString(
      const std::string &str_to_split) {
    std::vector<oid_t> store;
    std::size_t pos = 0, found;
    while ((found = str_to_split.find_first_of(':', pos)) !=
           std::string::npos) {
      store.push_back((oid_t)std::stoul(str_to_split.substr(pos, found - pos)));
      pos = found + 1;
    }
    return std::make_tuple(store.at(0), store.at(1), store.at(2));
  }

  std::string GetStringFromIndexConfig(
      const brain::IndexConfiguration &config) {
    std::ostringstream str_stream;
    auto config_indexes = config.GetIndexes();
    for (const auto &index_obj : config_indexes) {
      str_stream << index_obj->db_oid << ":" << index_obj->table_oid;
      for (auto column_oid : index_obj->column_oids) {
        str_stream << ":" << column_oid;
      }
      str_stream << ";";
    }
    return str_stream.str();
  }

  std::shared_ptr<brain::IndexObject> GetIndexObjectFromString(
      const std::string &str_to_split) {
    std::vector<oid_t> store;
    std::size_t pos = 0, found;
    while ((found = str_to_split.find_first_of(':', pos)) !=
           std::string::npos) {
      store.push_back((oid_t)std::stoul(str_to_split.substr(pos, found - pos)));
      pos = found + 1;
    }
    oid_t db_oid = store.at(0);
    oid_t table_oid = store.at(1);
    store.erase(store.begin(), store.begin() + 2);
    return std::make_shared<brain::IndexObject>(db_oid, table_oid, store);
  }

  std::shared_ptr<brain::IndexConfiguration> GetIndexConfigFromString(
      const std::string &str_to_split) {
    std::set<std::shared_ptr<brain::IndexObject>> index_obj_set;
    std::size_t pos = 0, found;
    while ((found = str_to_split.find_first_of(';', pos)) !=
           std::string::npos) {
      index_obj_set.insert(
          GetIndexObjectFromString(str_to_split.substr(pos, found - pos)));
      pos = found + 1;
    }
    return std::make_shared<brain::IndexConfiguration>(index_obj_set);
  }

  void InsertNextColumnToMap(const std::tuple<oid_t, oid_t, oid_t> &col) {
    auto col_str = GetStringFromTriplet(std::get<0>(col), std::get<1>(col),
                                        std::get<2>(col));
    column_id_map_[col_str] = next_column_id_;
    id_column_map_[next_column_id_++] = col_str;
  }

  void InsertNextConfigToMap(const brain::IndexConfiguration &config) {
    auto config_str = GetStringFromIndexConfig(config);
    config_id_map_[config_str] = next_config_id_;
    id_config_map_[next_config_id_++] = config_str;
  }

  void GenerateColumnIdMap() {
    column_id_map_.clear();
    id_column_map_.clear();
    auto all_columns = GetAllColumns();
    for (const auto &it : all_columns) {
      InsertNextColumnToMap(it);
    }
  }

  void EnumerateNColumns(const std::vector<oid_t> &col_oids,
                         std::vector<std::vector<oid_t>> &enumeration,
                         std::vector<oid_t> &store, size_t start, size_t end,
                         size_t idx, size_t n) {
    if (idx == n) {
      enumeration.emplace_back(store);
      return;
    }

    for (size_t i = start; i <= end && end - i + 1 >= n - idx; ++i) {
      store.push_back(col_oids.at(i));
      EnumerateNColumns(col_oids, enumeration, store, i + 1, end, idx + 1, n);
      store.pop_back();
    }
  }

  std::vector<std::vector<oid_t>> EnumerateAllColumns(
      const std::vector<oid_t> &col_oids) {
    std::vector<oid_t> store;
    std::vector<std::vector<oid_t>> enumeration;
    enumeration.emplace_back();

    for (size_t i = 1; i <= col_oids.size(); ++i) {
      EnumerateNColumns(col_oids, enumeration, store, 0, col_oids.size() - 1, 0,
                        i);
    }

    return enumeration;
  }

  void GenerateConfigIdMap() {
    // TODO: Generate all possible index configurations
    config_id_map_.clear();
    id_config_map_.clear();

    auto txn = txn_manager_->BeginTransaction();

    const auto db_object = catalog_->GetDatabaseObject(database_name_, txn);
    oid_t db_oid = db_object->GetDatabaseOid();
    const auto table_objects = db_object->GetTableObjects();

    LOG_DEBUG("db:%d", (int)db_oid);

    for (const auto &it : table_objects) {
      oid_t table_oid = it.first;
      LOG_DEBUG("table:%d", (int)table_oid);
      const auto table_obj = it.second;
      const auto column_objects = table_obj->GetColumnObjects();
      std::vector<oid_t> col_oids;
      for (const auto &col_it : column_objects) {
        oid_t col_oid = col_it.first;
        col_oids.push_back(col_oid);
      }
      const auto enumeration = EnumerateAllColumns(col_oids);
      for (const auto &each : enumeration) {
        std::ostringstream str_stream;
        for (const auto cur : each) {
          str_stream << cur << " ";
        }
        LOG_DEBUG("--%s", str_stream.str().c_str());
      }
    }

    txn_manager_->CommitTransaction(txn);
  }

  bool GetColumnMapId(const std::tuple<oid_t, oid_t, oid_t> &col,
                      oid_t &col_id) {
    auto col_str = GetStringFromTriplet(std::get<0>(col), std::get<1>(col),
                                        std::get<2>(col));
    auto it = column_id_map_.find(col_str);
    if (it == column_id_map_.end()) {
      return false;
    }
    col_id = it->second;
    return true;
  }

  bool GetIdMapColumn(const oid_t col_id,
                      std::tuple<oid_t, oid_t, oid_t> &col) {
    auto it = id_column_map_.find(col_id);
    if (it == id_column_map_.end()) {
      return false;
    }
    col = GetTripletFromString(it->second);
    return true;
  }

  bool GetConfigMapId(const brain::IndexConfiguration &config,
                      oid_t &config_id) {
    auto config_str = GetStringFromIndexConfig(config);
    auto it = config_id_map_.find(config_str);
    if (it == config_id_map_.end()) {
      return false;
    }
    config_id = it->second;
    return true;
  }

  bool GetIdMapConfig(
      const oid_t config_id,
      std::shared_ptr<brain::IndexConfiguration> &index_config) {
    auto it = id_config_map_.find(config_id);
    if (it == id_config_map_.end()) {
      return false;
    }
    index_config = GetIndexConfigFromString(it->second);
    return true;
  }

  oid_t GetNextColumnId() { return next_column_id_; }
  oid_t GetNextConfigId() { return next_config_id_; }
};

TEST_F(RLFrameworkTest, BasicTest) {
  std::string database_name = DEFAULT_DB_NAME;
  std::string table_name_1 = "dummy_table_1";
  std::string table_name_2 = "dummy_table_2";

  CreateDatabase(database_name);
  CreateTable(table_name_1);
  CreateTable(table_name_2);

  auto all_columns = GetAllColumns();
  LOG_DEBUG("All columns:");
  for (const auto &it : all_columns) {
    LOG_DEBUG("column [%d, %d, %d]", (int)std::get<0>(it), (int)std::get<1>(it),
              (int)std::get<2>(it));
  }

  auto all_indexes = GetAllIndexes();
  LOG_DEBUG("All indexes:");
  for (const auto &it : all_indexes) {
    LOG_DEBUG("index [%d, %d, %d]", (int)std::get<0>(it), (int)std::get<1>(it),
              (int)std::get<2>(it));
  }

  GenerateColumnIdMap();

  GenerateConfigIdMap();
}

}  // namespace test
}  // namespace peloton
