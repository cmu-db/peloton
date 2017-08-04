//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// settings_util.cpp
//
// Identification: src/settings/settings_util.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <gflags/gflags.h>

#include "type/value_factory.h"
#include "settings/settings_util.h"
#include "settings/settings_manager.h"

// gflags DECLARE
#define __SETTING_GFLAGS_DECLARE__
#include "settings/settings_macro.h"
#include "settings/settings.h"
#undef __SETTING_GFLAGS_DECLARE__

#define __SETTING_DEFINE__
#include "settings/settings_macro.h"

namespace peloton {
namespace settings {

int32_t SettingsUtil::GetInt(SettingsId id) {
  return SettingsManager::GetInstance()->GetValue(id).GetAs<int32_t>();
}

bool SettingsUtil::GetBool(SettingsId id) {
  return SettingsManager::GetInstance()->GetValue(id).GetAs<bool>();
}

std::string SettingsUtil::GetString(SettingsId id) {
  return SettingsManager::GetInstance()->GetValue(id).ToString();
}

void SettingsUtil::SetInt(SettingsId id, int32_t value) {
  SettingsManager::GetInstance()->SetValue(
      id, type::ValueFactory::GetIntegerValue(value));
}

void SettingsUtil::SetBool(SettingsId id, bool value) {
  SettingsManager::GetInstance()->SetValue(
      id, type::ValueFactory::GetBooleanValue(value));
}

void SettingsUtil::SetString(SettingsId id, const std::string &value) {
  SettingsManager::GetInstance()->SetValue(
      id, type::ValueFactory::GetVarcharValue(value));
}

}  // namespace settings
}  // namespace peloton

#undef __SETTING_DEFINE__
