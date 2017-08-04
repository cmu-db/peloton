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

#include "common/exception.h"
#include "type/ephemeral_pool.h"
#include "type/value_factory.h"
#include "catalog/settings_catalog.h"
#include "concurrency/transaction_manager_factory.h"
#include "settings/settings_util.h"
#include "settings/settings_manager.h"

#define __SETTING_GFLAGS_DECLARE__
#include "settings/settings_macro.h"
#include "settings/settings.h"
#undef __SETTING_GFLAGS_DECLARE__

#define __SETTING_DEFINE__
#include "settings/settings_macro.h"
#undef __SETTING_DEFINE__

namespace peloton {
namespace settings {

SettingsManager* SettingsManager::GetInstance() {
  static std::unique_ptr<SettingsManager> config_manager(
          new SettingsManager());
  return config_manager.get();
}

void SettingsManager::InitializeCatalog() {
  auto config_catalog = peloton::catalog::SettingsCatalog::GetInstance();

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  type::AbstractPool *pool = pool_.get();

  for (auto s : settings) {
    // TODO: Use Update instead Delete & Insert
    config_catalog->DeleteSetting(s.second.name, txn);
    if (!config_catalog->InsertSetting(s.second.name, s.second.value.ToString(),
                                       s.second.value.GetTypeId(),
                                       s.second.desc, "", "", s.second.default_value.ToString(),
                                       s.second.is_mutable, s.second.is_persistent,
                                       pool, txn)) {
      txn_manager.AbortTransaction(txn);
      throw SettingsException("failed to initialize catalog pg_settings on " +
                              s.second.name);
    }
  }
  txn_manager.CommitTransaction(txn);
  catalog_initialized = true;
}

const std::string SettingsManager::GetInfo() const {
  std::ostringstream str;
  str << "//===-------------- PELOTON SETTINGURATION --------------===//" << std::endl;
  str << std::endl;

  str << "Port: " << SettingsUtil::GetInt(SettingsId::port) << std::endl;
  str << "Socket Family: " << SettingsUtil::GetString(SettingsId::socket_family) << std::endl;
  str << "Statistics: " << (SettingsUtil::GetInt(SettingsId::stats_mode) ? "enabled" : "disabled") << std::endl;
  str << "Max Connections: " << SettingsUtil::GetInt(SettingsId::max_connections) << std::endl;
  str << "Index Tuner: " << (SettingsUtil::GetBool(SettingsId::index_tuner) ? "enabled" : "disabled") << std::endl;
  str << "Layout Tuner: " << (SettingsUtil::GetBool(SettingsId::layout_tuner) ? "enabled" : "disabled") << std::endl;
  str << "Code-generation: " << (SettingsUtil::GetBool(SettingsId::codegen) ? "enabled" : "disabled") << std::endl;

  str << std::endl;
  str << "//===---------------------------------------------------===//" << std::endl;

  return str.str();
}

void SettingsManager::ShowInfo() {
  LOG_INFO("%s", GetInfo().c_str());
}

void SettingsManager::DefineSetting(
    SettingsId id, const std::string &name,
    const type::Value &value,
    const std::string &description,
    const type::Value &default_value,
    bool is_mutable, bool is_persistent) {
  if (settings.find(id) != settings.end()) {
    throw SettingsException("settings " + name + " already exists");
  }
  settings.emplace(id, Param(name, value, description, default_value,
                     is_mutable, is_persistent));
}

type::Value SettingsManager::GetValue(SettingsId id) {
  // TODO: Look up the value from catalog
  // Because querying a catalog table needs to create a new transaction and
  // creating transaction needs to get config values,
  // it will be a infinite recursion here.

  auto param = settings.find(id);
  return param->second.value;
}

void SettingsManager::SetValue(SettingsId id, const type::Value &value) {
  auto param = settings.find(id);
  Param new_param = param->second;
  new_param.value = value;
  if (catalog_initialized) {
    if (!InsertIntoCatalog(new_param)) {
      throw SettingsException("failed to set value " + param->second.name);
    }
  }
  param->second.value = value;
}

bool SettingsManager::InsertIntoCatalog(const Param &param) {
  auto config_catalog = catalog::SettingsCatalog::GetInstance();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  type::AbstractPool *pool = pool_.get();
  // TODO: Use Update instead Delete & Insert
  config_catalog->DeleteSetting(param.name, txn);
  if (!config_catalog->InsertSetting(param.name, param.value.ToString(),
                                     param.value.GetTypeId(),
                                     param.desc, "", "", param.default_value.ToString(),
                                     param.is_mutable, param.is_persistent,
                                     pool, txn)) {
    txn_manager.AbortTransaction(txn);
    return false;
  }
  txn_manager.CommitTransaction(txn);
  return true;
}

SettingsManager::SettingsManager() {
  catalog_initialized = false;
  pool_.reset(new type::EphemeralPool());
  #include "settings/settings.h"
}

} // namespace settings
} // namespace peloton

