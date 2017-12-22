//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// settings_catalog.cpp
//
// Identification: src/catalog/settings_catalog.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/settings_catalog.h"
#include "catalog/catalog.h"
#include "executor/logical_tile.h"
#include "storage/data_table.h"
#include "type/value_factory.h"

#define SETTINGS_CATALOG_NAME "pg_settings"

namespace peloton {
namespace catalog {

SettingsCatalog &SettingsCatalog::GetInstance(concurrency::TransactionContext *txn) {
  static SettingsCatalog settings_catalog{txn};
  return settings_catalog;
}

SettingsCatalog::SettingsCatalog(concurrency::TransactionContext *txn)
    : AbstractCatalog("CREATE TABLE " CATALOG_DATABASE_NAME
                      "." SETTINGS_CATALOG_NAME
                      " ("
                      "name   VARCHAR NOT NULL, "
                      "value  VARCHAR NOT NULL, "
                      "value_type   VARCHAR NOT NULL, "
                      "description  VARCHAR, "
                      "min_value    VARCHAR, "
                      "max_value    VARCHAR, "
                      "default_value    VARCHAR NOT NULL, "
                      "is_mutable   BOOL NOT NULL, "
                      "is_persistent  BOOL NOT NULL);",
                      txn) {
  // Add secondary index here if necessary
  Catalog::GetInstance()->CreateIndex(
      CATALOG_DATABASE_NAME, SETTINGS_CATALOG_NAME, {0},
      SETTINGS_CATALOG_NAME "_skey0", false, IndexType::BWTREE, txn);
}

SettingsCatalog::~SettingsCatalog() {}

bool SettingsCatalog::InsertSetting(
    const std::string &name, const std::string &value, type::TypeId value_type,
    const std::string &description, const std::string &min_value,
    const std::string &max_value, const std::string &default_value,
    bool is_mutable, bool is_persistent, type::AbstractPool *pool,
    concurrency::TransactionContext *txn) {
  // Create the tuple first
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val0 = type::ValueFactory::GetVarcharValue(name, pool);
  auto val1 = type::ValueFactory::GetVarcharValue(value, pool);
  auto val2 =
      type::ValueFactory::GetVarcharValue(TypeIdToString(value_type), pool);
  auto val3 = type::ValueFactory::GetVarcharValue(description, pool);
  auto val4 = type::ValueFactory::GetVarcharValue(min_value, pool);
  auto val5 = type::ValueFactory::GetVarcharValue(max_value, pool);
  auto val6 = type::ValueFactory::GetVarcharValue(default_value, pool);
  auto val7 = type::ValueFactory::GetBooleanValue(is_mutable);
  auto val8 = type::ValueFactory::GetBooleanValue(is_persistent);

  tuple->SetValue(static_cast<int>(ColumnId::NAME), val0, pool);
  tuple->SetValue(static_cast<int>(ColumnId::VALUE), val1, pool);
  tuple->SetValue(static_cast<int>(ColumnId::VALUE_TYPE), val2, pool);
  tuple->SetValue(static_cast<int>(ColumnId::DESCRIPTION), val3, pool);
  tuple->SetValue(static_cast<int>(ColumnId::MIN_VALUE), val4, pool);
  tuple->SetValue(static_cast<int>(ColumnId::MAX_VALUE), val5, pool);
  tuple->SetValue(static_cast<int>(ColumnId::DEFAULT_VALUE), val6, pool);
  tuple->SetValue(static_cast<int>(ColumnId::IS_MUTABLE), val7, pool);
  tuple->SetValue(static_cast<int>(ColumnId::IS_PERSISTENT), val8, pool);

  // Insert the tuple
  return InsertTuple(std::move(tuple), txn);
}

bool SettingsCatalog::DeleteSetting(const std::string &name,
                                    concurrency::TransactionContext *txn) {
  oid_t index_offset = 0;
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetVarcharValue(name, nullptr).Copy());

  return DeleteWithIndexScan(index_offset, values, txn);
}

std::string SettingsCatalog::GetSettingValue(const std::string &name,
                                             concurrency::TransactionContext *txn) {
  std::vector<oid_t> column_ids({static_cast<int>(ColumnId::VALUE)});
  oid_t index_offset = static_cast<int>(IndexId::SECONDARY_KEY_0);
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetVarcharValue(name, nullptr).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  std::string config_value = "";
  PL_ASSERT(result_tiles->size() <= 1);
  if (result_tiles->size() != 0) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      config_value = (*result_tiles)[0]->GetValue(0, 0).ToString();
    }
  }
  return config_value;
}

std::string SettingsCatalog::GetDefaultValue(const std::string &name,
                                             concurrency::TransactionContext *txn) {
  std::vector<oid_t> column_ids({static_cast<int>(ColumnId::VALUE)});
  oid_t index_offset = static_cast<int>(IndexId::SECONDARY_KEY_0);
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetVarcharValue(name, nullptr).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  std::string config_value = "";
  PL_ASSERT(result_tiles->size() <= 1);
  if (result_tiles->size() != 0) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      config_value = (*result_tiles)[0]->GetValue(0, 0).ToString();
    }
  }
  return config_value;
}

}  // namespace catalog
}  // namespace peloton
