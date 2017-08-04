//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// settings_util.h
//
// Identification: src/include/settings/settings_util.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "settings/settings_type.h"

namespace peloton {
namespace settings {

class SettingsUtil {
public:
  static int32_t GetInt(SettingsId id);
  static bool GetBool(SettingsId id);
  static std::string GetString(SettingsId id);

  static void SetInt(SettingsId id, int32_t value);
  static void SetBool(SettingsId id, bool value);
  static void SetString(SettingsId id, const std::string &value);
};

}  // namespace settings
}  // namespace peloton
