//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// configuration_manager.cpp
//
// Identification: src/configuration/configuration_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#define __PELOTON_CONFIG__

#include "common/exception.h"
#include "configuration/configuration_manager.h"
#include "configuration/configuration.h"
#include "catalog/config_catalog.h"

namespace peloton {
namespace configuration {

ConfigurationManager* ConfigurationManager::GetInstance() {
  static std::unique_ptr<ConfigurationManager> config_manager(
          new ConfigurationManager());
  return config_manager.get();
}

template<typename T>
T ConfigurationManager::GetValue(const std::string &name) {
  auto param = config.find(name);
  if (param == config.end()) {
    throw new Exception("no such configuration: " + name);
  }
  if (param->second.value_type != type::TypeId::BOOLEAN) {
    throw new Exception("configuration " + name + " is not a bool");
  }
  return to_value<T>(param->second.value, param->second.value_type);
}

template<typename T>
void ConfigurationManager::SetValue(const std::string &name, const T &value) {
  auto param = config.find(name);
  if (param == config.end()) {
    throw new Exception("no such configuration: " + name);
  }
  if (param->second.value_type != type::TypeId::BOOLEAN) {
    throw new Exception("configuration " + name + " is not a bool");
  }
  param->second.value = to_string(value);
}

template<typename T>
void ConfigurationManager::DefineConfig(const std::string &name, void* value, type::TypeId type,
                                        const std::string &description, const T &default_value,
                                        bool is_mutable, bool is_persistent) {
  if (config.count(name) > 0) {
    throw Exception("configuration " + name + " already exists");
  }
  std::string value_str = "";
  switch (type) {
    case type::TypeId::INTEGER:
      break;
    case type::TypeId::BOOLEAN:
      break;
    case type::TypeId::VARCHAR
      break;
  }
  config[name] = Param(value_str,, description, type::TypeId::BOOLEAN,
                       to_string(default_value), is_mutable, is_persistent);
}

void ConfigurationManager::InitializeCatalog() {
  peloton::catalog::ConfigCatalog* config_catalog = peloton::catalog::ConfigCatalog::GetInstance();

  auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  type::AbstractPool *pool = pool_.get();

  for (auto conf : config) {
    config_catalog->InsertConfig(conf.first, conf.second.value, conf.second.value_type,
                                 conf.second.desc, "", "", conf.second.default_value,
                                 conf.second.is_mutable, conf.second.is_persistent,
                                 pool, txn);
  }

  txn_manager.CommitTransaction(txn);
}

void ConfigurationManager::PrintConfiguration() {
  LOG_INFO("%30s", "//===-------------- PELOTON CONFIGURATION --------------===//");
  LOG_INFO(" ");

  LOG_INFO("%30s: %10llu", "Port", GET_INT("port"));
  LOG_INFO("%30s: %10s", "Socket Family", GET_STRING("socket_family").c_str());
  LOG_INFO("%30s: %10s", "Statistics", GET_INT("stats_mode") ? "enabled" : "disabled");
  LOG_INFO("%30s: %10llu", "Max Connections", GET_INT("max_connections"));
  LOG_INFO("%30s: %10s", "Index Tuner", GET_BOOL("index_tuner") ? "enabled" : "disabled");
  LOG_INFO("%30s: %10s", "Layout Tuner", GET_BOOL("layout_tuner") ? "enabled" : "disabled");
  LOG_INFO("%30s: %10s", "Code-generation", GET_BOOL("codegen") ? "enabled" : "disabled");

  LOG_INFO(" ");
  LOG_INFO("%30s", "//===---------------------------------------------------===//");
}

}
}

