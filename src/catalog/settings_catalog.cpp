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

SettingsCatalogEntry::SettingsCatalogEntry(executor::LogicalTile *tile,
                                           int tuple_id)
    : name_(tile->GetValue(tuple_id,
                           static_cast<int>(SettingsCatalog::ColumnId::NAME))
                .ToString()),
      value_type_(StringToTypeId(
          tile->GetValue(tuple_id, static_cast<int>(
                                       SettingsCatalog::ColumnId::VALUE_TYPE))
              .ToString())),
      desc_(tile->GetValue(
                      tuple_id,
                      static_cast<int>(SettingsCatalog::ColumnId::DESCRIPTION))
                .ToString()),
      is_mutable_(
          tile->GetValue(tuple_id, static_cast<int>(
                                       SettingsCatalog::ColumnId::IS_MUTABLE))
              .GetAs<bool>()),
      is_persistent_(
          tile->GetValue(
                    tuple_id,
                    static_cast<int>(SettingsCatalog::ColumnId::IS_PERSISTENT))
              .GetAs<bool>()) {
  switch (value_type_) {
    case type::TypeId::INTEGER: {
      value_ = type::ValueFactory::GetIntegerValue(std::stoi(
          tile->GetValue(tuple_id,
                         static_cast<int>(SettingsCatalog::ColumnId::VALUE))
              .ToString()));
      default_value_ = type::ValueFactory::GetIntegerValue(std::stoi(
          tile->GetValue(
                    tuple_id,
                    static_cast<int>(SettingsCatalog::ColumnId::DEFAULT_VALUE))
              .ToString()));
      min_value_ = type::ValueFactory::GetIntegerValue(std::stoi(
          tile->GetValue(tuple_id,
                         static_cast<int>(SettingsCatalog::ColumnId::MIN_VALUE))
              .ToString()));
      max_value_ = type::ValueFactory::GetIntegerValue(std::stoi(
          tile->GetValue(tuple_id,
                         static_cast<int>(SettingsCatalog::ColumnId::MAX_VALUE))
              .ToString()));
      break;
    }
    case type::TypeId::DECIMAL: {
      value_ = type::ValueFactory::GetDecimalValue(std::stof(
          tile->GetValue(tuple_id,
                         static_cast<int>(SettingsCatalog::ColumnId::VALUE))
              .ToString()));
      default_value_ = type::ValueFactory::GetDecimalValue(std::stof(
          tile->GetValue(
                    tuple_id,
                    static_cast<int>(SettingsCatalog::ColumnId::DEFAULT_VALUE))
              .ToString()));
      min_value_ = type::ValueFactory::GetDecimalValue(std::stof(
          tile->GetValue(tuple_id,
                         static_cast<int>(SettingsCatalog::ColumnId::MIN_VALUE))
              .ToString()));
      max_value_ = type::ValueFactory::GetDecimalValue(std::stof(
          tile->GetValue(tuple_id,
                         static_cast<int>(SettingsCatalog::ColumnId::MAX_VALUE))
              .ToString()));
      break;
    }
    case type::TypeId::BOOLEAN: {
      value_ = type::ValueFactory::GetBooleanValue(
          (tile->GetValue(tuple_id,
                          static_cast<int>(SettingsCatalog::ColumnId::VALUE))
               .ToString() == "true")
              ? true
              : false);
      default_value_ = type::ValueFactory::GetBooleanValue(
          (tile->GetValue(
                     tuple_id,
                     static_cast<int>(SettingsCatalog::ColumnId::DEFAULT_VALUE))
               .ToString() == "true")
              ? true
              : false);
      break;
    }
    case type::TypeId::VARCHAR: {
      value_ = tile->GetValue(
          tuple_id, static_cast<int>(SettingsCatalog::ColumnId::VALUE));
      default_value_ = tile->GetValue(
          tuple_id, static_cast<int>(SettingsCatalog::ColumnId::DEFAULT_VALUE));
      break;
    }
    default:
      LOG_ERROR("Unsupported type for setting value: %s",
                TypeIdToString(value_type_).c_str());
      PELOTON_ASSERT(false);
  }
}

SettingsCatalog &SettingsCatalog::GetInstance(
    concurrency::TransactionContext *txn) {
  static SettingsCatalog settings_catalog{txn};
  return settings_catalog;
}

SettingsCatalog::SettingsCatalog(concurrency::TransactionContext *txn)
    : AbstractCatalog("CREATE TABLE " CATALOG_DATABASE_NAME
                      "." CATALOG_SCHEMA_NAME "." SETTINGS_CATALOG_NAME
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
      CATALOG_DATABASE_NAME, CATALOG_SCHEMA_NAME, SETTINGS_CATALOG_NAME, {0},
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

/** @brief      Update a setting catalog entry corresponding to a name
 *              in the pg_settings.
 *  @param      name  name of the setting.
 *  @param      value  value for the updating.
 *  @param      txn  TransactionContext for getting the setting.
 *  @return     setting catalog entry.
 */
bool SettingsCatalog::UpdateSettingValue(concurrency::TransactionContext *txn,
                                         const std::string &name,
                                         const std::string &value,
                                         bool set_default) {
  std::vector<oid_t> update_columns(
      {static_cast<int>(ColumnId::VALUE)});  // value
  oid_t index_offset =
      static_cast<int>(IndexId::SECONDARY_KEY_0);  // Index of name
  // values to execute index scan
  std::vector<type::Value> scan_values;
  scan_values.push_back(type::ValueFactory::GetVarcharValue(name).Copy());
  // values to update
  std::vector<type::Value> update_values;
  update_values.push_back(type::ValueFactory::GetVarcharValue(value).Copy());

  if (set_default) {
    update_columns.push_back(static_cast<oid_t>(ColumnId::DEFAULT_VALUE));
    update_values.push_back(type::ValueFactory::GetVarcharValue(value).Copy());
  }

  return UpdateWithIndexScan(update_columns, update_values, scan_values,
                             index_offset, txn);
}

/** @brief      Get a setting catalog entry corresponding to a name
 *              from the pg_settings.
 *  @param      name  name of the setting.
 *  @param      txn  TransactionContext for getting the setting.
 *  @return     setting catalog entry.
 */
std::shared_ptr<SettingsCatalogEntry> SettingsCatalog::GetSetting(
    const std::string &name, concurrency::TransactionContext *txn) {
  if (txn == nullptr) {
    throw CatalogException("Transaction is invalid!");
  }

  std::vector<oid_t> column_ids(all_column_ids_);
  oid_t index_offset = static_cast<int>(IndexId::SECONDARY_KEY_0);
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetVarcharValue(name, nullptr).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  PELOTON_ASSERT(result_tiles->size() <= 1);
  if (result_tiles->size() != 0) {
    PELOTON_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      return std::make_shared<SettingsCatalogEntry>((*result_tiles)[0].get());
    }
  }

  return nullptr;
}

/** @brief      Get all setting catalog entries from the pg_settings.
 *  @param      txn TransactionContext for getting the settings.
 *  @return     unordered_map containing a name -> setting catalog
 *              entry mapping.
 */
std::unordered_map<std::string, std::shared_ptr<SettingsCatalogEntry>>
SettingsCatalog::GetSettings(concurrency::TransactionContext *txn) {
  if (txn == nullptr) {
    throw CatalogException("Transaction is invalid!");
  }

  std::vector<oid_t> column_ids(all_column_ids_);

  auto result_tiles = this->GetResultWithSeqScan(column_ids, nullptr, txn);

  std::unordered_map<std::string, std::shared_ptr<SettingsCatalogEntry>>
      setting_entries;
  for (auto &tile : (*result_tiles)) {
    for (auto tuple_id : *tile) {
      auto setting_entry =
          std::make_shared<SettingsCatalogEntry>(tile.get(), tuple_id);
      setting_entries.insert(
          std::make_pair(setting_entry->GetName(), setting_entry));
    }
  }

  return setting_entries;
}

}  // namespace catalog
}  // namespace peloton
