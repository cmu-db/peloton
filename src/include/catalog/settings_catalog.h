//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// settings_catalog.h
//
// Identification: src/include/catalog/settings_catalog.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/abstract_catalog.h"

namespace peloton {
namespace catalog {

//===----------------------------------------------------------------------===//
// In-memory representation of a row from the pg_settings table.
//===----------------------------------------------------------------------===//
class SettingsCatalogEntry {
 public:
  SettingsCatalogEntry(executor::LogicalTile *tile, int tuple_id = 0);

  // Accessors
  inline const std::string &GetName() { return name_; }
  inline type::Value &GetValue() { return value_; }
  inline type::TypeId GetValueType() { return value_type_; }
  inline const std::string &GetDescription() { return desc_; }
  inline type::Value &GetDefaultValue() { return default_value_; }
  inline type::Value &GetMinValue() { return min_value_; }
  inline type::Value &GetMaxValue() { return max_value_; }
  inline bool IsMutable() { return is_mutable_; }
  inline bool IsPersistent() { return is_persistent_; }

 private:
  // The fields of 'pg_settings' table
  std::string name_;
  type::Value value_;
  type::TypeId value_type_;
  std::string desc_;
  type::Value default_value_;
  type::Value min_value_;
  type::Value max_value_;
  bool is_mutable_, is_persistent_;
};

//===----------------------------------------------------------------------===//
// The pg_settings catalog table.
//===----------------------------------------------------------------------===//
class SettingsCatalog : public AbstractCatalog {
 public:
  ~SettingsCatalog();

  // Global Singleton
  static SettingsCatalog &GetInstance(
      concurrency::TransactionContext *txn = nullptr);

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool InsertSetting(const std::string &name, const std::string &value,
                     type::TypeId value_type, const std::string &description,
                     const std::string &min_value, const std::string &max_value,
                     const std::string &default_value, bool is_mutable,
                     bool is_persistent, type::AbstractPool *pool,
                     concurrency::TransactionContext *txn);

  bool DeleteSetting(const std::string &name,
                     concurrency::TransactionContext *txn);

  bool UpdateSettingValue(concurrency::TransactionContext *txn,
                          const std::string &name, const std::string &value,
                          bool set_default);

  //===--------------------------------------------------------------------===//
  // Read-only Related API
  //===--------------------------------------------------------------------===//
  std::shared_ptr<SettingsCatalogEntry> GetSettingsCatalogEntry(
      const std::string &name, concurrency::TransactionContext *txn);

  std::unordered_map<std::string, std::shared_ptr<SettingsCatalogEntry>>
  GetSettingsCatalogEntries(concurrency::TransactionContext *txn);

  enum class ColumnId {
    NAME = 0,
    VALUE = 1,
    VALUE_TYPE = 2,
    DESCRIPTION = 3,
    MIN_VALUE = 4,
    MAX_VALUE = 5,
    DEFAULT_VALUE = 6,
    IS_MUTABLE = 7,
    IS_PERSISTENT = 8,
    // Add new columns here in creation order
  };
  std::vector<oid_t> all_column_ids_ = {0, 1, 2, 3, 4, 5, 6, 7, 8};

 private:
  SettingsCatalog(concurrency::TransactionContext *txn);

  enum class IndexId {
    SECONDARY_KEY_0 = 0,
    // Add new indexes here in creation order
  };
};

}  // namespace catalog
}  // namespace peloton
