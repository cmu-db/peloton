//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// settings_manager.h
//
// Identification: src/include/settings/settings_manager.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <unordered_map>
#include "common/internal_types.h"
#include "type/value.h"
#include "common/exception.h"
#include "common/printable.h"
#include "concurrency/transaction_context.h"
#include "settings/setting_id.h"

namespace peloton {
namespace settings {


// SettingsManager:
// provide ability to define, get_value and set_value of setting parameters
// It stores information in an internal map as well as catalog pg_settings
class SettingsManager : public Printable {
 public:
  static SettingsManager &GetInstance();

  // Getter functions for setting value.
  int32_t GetInt(SettingId id) const;
  double GetDouble(SettingId id) const;
  bool GetBool(SettingId id) const;
  std::string GetString(SettingId id) const;
  type::Value GetValue(SettingId id) const;

  // Setter functions to update internal setting value.
  // These can be called without txn before calling InitializeCatalog().
  // If set_default is true, then set default_value in addition to value.
  void SetInt(SettingId id, int32_t value, bool set_default = false,
              concurrency::TransactionContext *txn = nullptr);
  void SetDouble(SettingId id, double value, bool set_default = false,
                 concurrency::TransactionContext *txn = nullptr);
  void SetBool(SettingId id, bool value, bool set_default = false,
               concurrency::TransactionContext *txn = nullptr);
  void SetString(SettingId id, const std::string &value,
                 bool set_default = false,
                 concurrency::TransactionContext *txn = nullptr);
  void SetValue(SettingId id, const type::Value &value,
                bool set_default = false,
                concurrency::TransactionContext *txn = nullptr);

  // Reset a setting value to a default value
  void ResetValue(SettingId id, concurrency::TransactionContext *txn);

  // Call this method in Catalog->Bootstrap
  // to store information into pg_settings
  void InitializeCatalog();

  void UpdateSettingListFromCatalog(concurrency::TransactionContext *txn);

  const std::string GetInfo() const;

  void ShowInfo();

 private:

  // local information storage
  // name, value, description, default_value, is_mutable, is_persistent
  struct Param {
    std::string name;
    type::Value value;
    std::string desc;
    type::Value default_value;
    type::Value min_value;
    type::Value max_value;
    bool is_mutable, is_persistent;

    Param(std::string name, const type::Value &value,
          std::string desc, const type::Value &default_value,
          type::Value min_value, type::Value max_value,
          bool is_mutable, bool is_persistent)
        : name(name), value(value), desc(desc),
          default_value(default_value), min_value(min_value),
          max_value(max_value), is_mutable(is_mutable),
          is_persistent(is_persistent) {}
  };

  // internal map
  struct EnumClassHash {
    template <typename T> std::size_t operator()(T t)
    const { return static_cast<std::size_t>(t); }
  };
  std::unordered_map<SettingId, Param, EnumClassHash> settings_;

  std::unique_ptr<type::AbstractPool> pool_;

  bool catalog_initialized_;

  SettingsManager();

  void DefineSetting(SettingId id, const std::string &name,
                     const type::Value &value,
                     const std::string &description,
                     const type::Value &default_value,
                     const type::Value &min_value,
                     const type::Value &max_value,
                     bool is_mutable, bool is_persistent);

  bool InsertCatalog(const Param &param,
                     concurrency::TransactionContext *txn);

  bool UpdateCatalog(const std::string &name, const type::Value &value,
                     bool set_default,
                     concurrency::TransactionContext *txn);
};

}  // namespace settings
}  // namespace peloton
