//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// settings_manager.cpp
//
// Identification: src/settings/settings_manager.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <gflags/gflags.h>

#include "catalog/settings_catalog.h"
#include "common/exception.h"
#include "concurrency/transaction_manager_factory.h"
#include "settings/settings_manager.h"
#include "type/ephemeral_pool.h"
#include "type/value_factory.h"
#include "util/stringbox_util.h"

// This will expand to define all the settings defined in settings.h
// using GFlag's DEFINE_...() macro. See settings_macro.h.
#define __SETTING_GFLAGS_DEFINE__
#include "settings/settings_macro.h"
#include "settings/settings.h"
#undef __SETTING_GFLAGS_DEFINE__

namespace peloton {
namespace settings {

int32_t SettingsManager::GetInt(SettingId id) {
  return GetInstance().GetValue(id).GetAs<int32_t>();
}

double SettingsManager::GetDouble(SettingId id) {
  return GetInstance().GetValue(id).GetAs<double>();
}

bool SettingsManager::GetBool(SettingId id) {
  return GetInstance().GetValue(id).GetAs<bool>();
}

std::string SettingsManager::GetString(SettingId id) {
  return GetInstance().GetValue(id).ToString();
}

void SettingsManager::SetInt(SettingId id, int32_t value) {
  GetInstance().SetValue(id, type::ValueFactory::GetIntegerValue(value));
}

void SettingsManager::SetBool(SettingId id, bool value) {
  GetInstance().SetValue(id, type::ValueFactory::GetBooleanValue(value));
}

void SettingsManager::SetString(SettingId id, const std::string &value) {
  GetInstance().SetValue(id, type::ValueFactory::GetVarcharValue(value));
}

SettingsManager &SettingsManager::GetInstance() {
  static SettingsManager settings_manager;
  return settings_manager;
}

void SettingsManager::InitializeCatalog() {
  auto &settings_catalog = peloton::catalog::SettingsCatalog::GetInstance();

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  type::AbstractPool *pool = pool_.get();

  for (auto s : settings_) {
    // TODO: Use Update instead Delete & Insert
    settings_catalog.DeleteSetting(s.second.name, txn);
    if (!settings_catalog.InsertSetting(
            s.second.name, s.second.value.ToString(),
            s.second.value.GetTypeId(), s.second.desc, "", "",
            s.second.default_value.ToString(), s.second.is_mutable,
            s.second.is_persistent, pool, txn)) {
      txn_manager.AbortTransaction(txn);
      throw SettingsException("failed to initialize catalog pg_settings on " +
                              s.second.name);
    }
  }
  txn_manager.CommitTransaction(txn);
  catalog_initialized_ = true;
}

const std::string SettingsManager::GetInfo() const {
  const uint32_t box_width = 60;
  const std::string title = "PELOTON SETTINGS";

  std::string info;
  info.append(StringUtil::Format("%*s\n", box_width/2 + title.length()/2, title.c_str()));
  info.append(StringUtil::Repeat("=", box_width)).append("\n");

  info.append(StringUtil::Format("%28s:   %-28i\n", "Port", GetInt(SettingId::port)));
  info.append(StringUtil::Format("%28s:   %-28s\n", "Socket Family", GetString(SettingId::socket_family).c_str()));
  info.append(StringUtil::Format("%28s:   %-28s\n", "Statistics", GetInt(SettingId::stats_mode) ? "enabled" : "disabled"));
  info.append(StringUtil::Format("%28s:   %-28i\n", "Max Connections", GetInt(SettingId::max_connections)));
  info.append(StringUtil::Format("%28s:   %-28s\n", "Index Tuner", GetBool(SettingId::index_tuner) ? "enabled" : "disabled"));
  info.append(StringUtil::Format("%28s:   %-28s\n", "Layout Tuner", GetBool(SettingId::layout_tuner) ? "enabled" : "disabled"));
  info.append(StringUtil::Format("%28s:   %-28s\n", "Code-generation", GetBool(SettingId::codegen) ? "enabled" : "disabled"));

  return StringBoxUtil::Box(info);
}

void SettingsManager::ShowInfo() { LOG_INFO("\n%s\n", GetInfo().c_str()); }

void SettingsManager::DefineSetting(SettingId id, const std::string &name,
                                    const type::Value &value,
                                    const std::string &description,
                                    const type::Value &default_value,
                                    bool is_mutable, bool is_persistent) {
  if (settings_.find(id) != settings_.end()) {
    throw SettingsException("settings " + name + " already exists");
  }
  settings_.emplace(id, Param(name, value, description, default_value,
                              is_mutable, is_persistent));
}

type::Value SettingsManager::GetValue(SettingId id) {
  // TODO: Look up the value from catalog
  // Because querying a catalog table needs to create a new transaction and
  // creating transaction needs to get setting values,
  // it will be a infinite recursion here.

  auto param = settings_.find(id);
  return param->second.value;
}

void SettingsManager::SetValue(SettingId id, const type::Value &value) {
  auto param = settings_.find(id);
  Param new_param = param->second;
  new_param.value = value;
  if (catalog_initialized_) {
    if (!InsertIntoCatalog(new_param)) {
      throw SettingsException("failed to set value " + param->second.name);
    }
  }
  param->second.value = value;
}

bool SettingsManager::InsertIntoCatalog(const Param &param) {
  auto &settings_catalog = catalog::SettingsCatalog::GetInstance();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  type::AbstractPool *pool = pool_.get();
  // TODO: Use Update instead Delete & Insert
  settings_catalog.DeleteSetting(param.name, txn);
  if (!settings_catalog.InsertSetting(
          param.name, param.value.ToString(), param.value.GetTypeId(),
          param.desc, "", "", param.default_value.ToString(), param.is_mutable,
          param.is_persistent, pool, txn)) {
    txn_manager.AbortTransaction(txn);
    return false;
  }
  txn_manager.CommitTransaction(txn);
  return true;
}

SettingsManager::SettingsManager() {
  catalog_initialized_ = false;
  pool_.reset(new type::EphemeralPool());

  // This will expand to invoke SettingsManager::DefineSetting on
  // all of the settings defined in settings.h. See settings_macro.h.
  #define __SETTING_DEFINE__
  #include "settings/settings_macro.h"
  #include "settings/settings.h"
  #undef __SETTING_DEFINE__
}

}  // namespace settings
}  // namespace peloton
