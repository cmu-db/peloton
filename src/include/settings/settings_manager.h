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
#include "settings/setting_id.h"

namespace peloton {
namespace settings {

// SettingsManager:
// provide ability to define, get_value and set_value of setting parameters
// It stores information in an internal map as well as catalog pg_settings
class SettingsManager : public Printable {
 public:
  static int32_t GetInt(SettingId id);
  static double GetDouble(SettingId id);
  static bool GetBool(SettingId id);
  static std::string GetString(SettingId id);

  static void SetInt(SettingId id, int32_t value);
  static void SetBool(SettingId id, bool value);
  static void SetString(SettingId id, const std::string &value);
  static SettingsManager &GetInstance();

  // Call this method in Catalog->Bootstrap
  // to store information into pg_settings
  void InitializeCatalog();

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
    bool is_mutable, is_persistent;

    Param(std::string name, const type::Value &value,
          std::string desc, const type::Value &default_value,
          bool is_mutable, bool is_persistent)
        : name(name), value(value), desc(desc),
          default_value(default_value),
          is_mutable(is_mutable), is_persistent(is_persistent) {}
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
                     bool is_mutable, bool is_persistent);

  type::Value GetValue(SettingId id);

  void SetValue(SettingId id, const type::Value &value);

  bool InsertIntoCatalog(const Param &param);
};

}  // namespace settings
}  // namespace peloton
