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

class SettingsCatalog : public AbstractCatalog {
 public:
  ~SettingsCatalog();

  // Global Singleton
  static SettingsCatalog &GetInstance(concurrency::TransactionContext *txn = nullptr);

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool InsertSetting(const std::string &name, const std::string &value,
                     type::TypeId value_type, const std::string &description,
                     const std::string &min_value, const std::string &max_value,
                     const std::string &default_value, bool is_mutable,
                     bool is_persistent, type::AbstractPool *pool,
                     concurrency::TransactionContext *txn);

  bool DeleteSetting(const std::string &name, concurrency::TransactionContext *txn);

  //===--------------------------------------------------------------------===//
  // Read-only Related API
  //===--------------------------------------------------------------------===//
  std::string GetSettingValue(const std::string &name,
                              concurrency::TransactionContext *txn);

  std::string GetDefaultValue(const std::string &name,
                              concurrency::TransactionContext *txn);

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

 private:
  SettingsCatalog(concurrency::TransactionContext *txn);

  enum class IndexId {
    SECONDARY_KEY_0 = 0,
    // Add new indexes here in creation order
  };
};

}  // namespace catalog
}  // namespace peloton
