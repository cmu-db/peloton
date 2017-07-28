//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// configuration_manager.cpp
//
// Identification: src/configuration/configuration_manager.cpp
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
#include "configuration/configuration_util.h"
#include "configuration/configuration_manager.h"

#define __CONFIG_GFLAGS_DECLARE__
#include "configuration/configuration_macro.h"
#include "configuration/configuration.h"
#undef __CONFIG_GFLAGS_DECLARE__
DECLARE_bool(help);

#define __CONFIG_DEFINE__
#include "configuration/configuration_macro.h"
#undef __CONFIG_DEFINE__

namespace peloton {
namespace configuration {

ConfigurationManager* ConfigurationManager::GetInstance() {
  static std::unique_ptr<ConfigurationManager> config_manager(
          new ConfigurationManager());
  return config_manager.get();
}

void ConfigurationManager::InitializeCatalog() {
  auto config_catalog = peloton::catalog::SettingsCatalog::GetInstance();

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  type::AbstractPool *pool = pool_.get();

  for (auto conf : config) {
    config_catalog->DeleteSetting(conf.second.name, txn);
    config_catalog->InsertSetting(conf.second.name, conf.second.value.ToString(),
                                  conf.second.value.GetTypeId(),
                                  conf.second.desc, "", "", conf.second.default_value.ToString(),
                                  conf.second.is_mutable, conf.second.is_persistent,
                                  pool, txn);
  }
  txn_manager.CommitTransaction(txn);
  catalog_initialized = true;
}

const std::string ConfigurationManager::GetInfo() const {
  std::ostringstream str;
  str << "//===-------------- PELOTON CONFIGURATION --------------===//" << std::endl;
  str << std::endl;

  str << "Port: " << ConfigurationUtil::GET_INT(ConfigurationId::port) << std::endl;
  str << "Socket Family: " << ConfigurationUtil::GET_STRING(ConfigurationId::socket_family) << std::endl;
  str << "Statistics: " << (ConfigurationUtil::GET_INT(ConfigurationId::stats_mode) ? "enabled" : "disabled") << std::endl;
  str << "Max Connections: " << ConfigurationUtil::GET_INT(ConfigurationId::max_connections) << std::endl;
  str << "Index Tuner: " << (ConfigurationUtil::GET_BOOL(ConfigurationId::index_tuner) ? "enabled" : "disabled") << std::endl;
  str << "Layout Tuner: " << (ConfigurationUtil::GET_BOOL(ConfigurationId::layout_tuner) ? "enabled" : "disabled") << std::endl;
  str << "Code-generation: " << (ConfigurationUtil::GET_BOOL(ConfigurationId::codegen) ? "enabled" : "disabled") << std::endl;

  str << std::endl;
  str << "//===---------------------------------------------------===//" << std::endl;

  return str.str();
}

void ConfigurationManager::ShowInfo() {
  LOG_INFO("%s", GetInfo().c_str());
}

void ConfigurationManager::DefineConfig(
    ConfigurationId id, const std::string &name,
    void* gflags_ptr, const std::string &description,
    const type::Value &value, const type::Value &default_value,
    bool is_mutable, bool is_persistent) {
  if (config.count(id) > 0) {
    throw ConfigurationException("configuration " + name + " already exists");
  }
  config[id] = Param(name, gflags_ptr, description,
                     value, default_value,
                     is_mutable, is_persistent);
}

type::Value ConfigurationManager::GetValue(ConfigurationId id) {
  auto param = config.find(id);
  return param->second.value;
}

void ConfigurationManager::SetValue(ConfigurationId id, const type::Value &value) {
  auto param = config.find(id);
  param->second.value = value;
  switch (value.GetTypeId()) {
    case type::TypeId::INTEGER:
      *(uint64_t*)param->second.gflags_ptr = value.GetAs<uint64_t>();
      break;
    case type::TypeId::BOOLEAN:
      *(bool*)param->second.gflags_ptr = value.GetAs<bool>();
      break;
    case type::TypeId::VARCHAR:
      *(std::string*)param->second.gflags_ptr = value.GetAs<const char*>();
      break;
    default:
      throw new ConfigurationException("unsupported type");
  }
  if (catalog_initialized) {
    insert_into_catalog(param->second);
  }
}

void ConfigurationManager::insert_into_catalog(const Param &param) {
  auto config_catalog = catalog::SettingsCatalog::GetInstance();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  type::AbstractPool *pool = pool_.get();
  config_catalog->DeleteSetting(param.name, txn);
  config_catalog->InsertSetting(param.name, param.value.ToString(),
                                param.value.GetTypeId(),
                                param.desc, "", "", param.default_value.ToString(),
                                param.is_mutable, param.is_persistent,
                                pool, txn);
  txn_manager.CommitTransaction(txn);
}

ConfigurationManager::ConfigurationManager() {
  catalog_initialized = false;
  pool_.reset(new type::EphemeralPool());
  DefineConfig(ConfigurationId::help, "help", &FLAGS_help, "Show help",
               type::ValueFactory::GetBooleanValue(FLAGS_help),
               type::ValueFactory::GetBooleanValue(false),
               false, false);
  #include "configuration/configuration.h"
}

} // End configuration namespace
} // End peloton namespace

