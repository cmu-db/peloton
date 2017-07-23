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

uint64_t GET_INT(const std::string& name) {
  return peloton::configuration::ConfigurationManager::GetInstance()->GetValue<uint64_t>(name);
}
bool GET_BOOL(const std::string& name) {
  return peloton::configuration::ConfigurationManager::GetInstance()->GetValue<bool>(name);
}
std::string GET_STRING(const std::string& name) {
  return peloton::configuration::ConfigurationManager::GetInstance()->GetValue<std::string>(name);
}

void SET_INT(const std::string &name, uint64_t value) {
  peloton::configuration::ConfigurationManager::GetInstance()->SetValue<uint64_t>(name, value);
}

void SET_BOOL(const std::string &name, bool value) {
  peloton::configuration::ConfigurationManager::GetInstance()->SetValue<bool>(name, value);
}
void SET_STRING(const std::string &name, const std::string &value) {
  peloton::configuration::ConfigurationManager::GetInstance()->SetValue<std::string>(name, value);
}

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
  return to_value<T>(param->second.value);
}

template<typename T>
void ConfigurationManager::SetValue(const std::string &name, const T &value) {
  auto param = config.find(name);
  if (param == config.end()) {
    throw new Exception("no such configuration: " + name);
  }
  switch (param->second.value_type) {
    case type::TypeId::INTEGER:
      *(uint64_t*)(param->second.value) = *(uint64_t*)(&value);
      break;
    case type::TypeId::BOOLEAN:
      *(bool*)(param->second.value) = *(bool*)(&value);
      break;
    case type::TypeId::VARCHAR:
      *(std::string*)(param->second.value) = *(std::string*)(&value);
      break;
    default:
      throw new Exception("unsupported type");
  }
  if (catalog_initialized) {
    insert_into_catalog(param->first, param->second);
  }
}

template<typename T>
void ConfigurationManager::DefineConfig(const std::string &name, void* value, type::TypeId type,
                                        const std::string &description, const T &default_value,
                                        bool is_mutable, bool is_persistent) {
  if (config.count(name) > 0) {
    throw Exception("configuration " + name + " already exists");
  }
  T tmp(default_value);
  config[name] = Param(value, description, type, to_string(&tmp, type),
                       is_mutable, is_persistent);
  if (catalog_initialized) {
    insert_into_catalog(name, config[name]);
  }
}

void ConfigurationManager::InitializeCatalog() {
  auto config_catalog = peloton::catalog::ConfigCatalog::GetInstance();

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  type::AbstractPool *pool = pool_.get();

  for (auto conf : config) {
    config_catalog->DeleteConfig(conf.first, txn);
    config_catalog->InsertConfig(conf.first, to_string(conf.second.value, conf.second.value_type),
                                 conf.second.value_type,
                                 conf.second.desc, "", "", conf.second.default_value,
                                 conf.second.is_mutable, conf.second.is_persistent,
                                 pool, txn);
  }
  txn_manager.CommitTransaction(txn);
  catalog_initialized = true;
}

void ConfigurationManager::PrintConfiguration() {
  LOG_INFO("%30s", "//===-------------- PELOTON CONFIGURATION --------------===//");
  LOG_INFO(" ");

  LOG_INFO("%30s: %10llu", "Port", (unsigned long long)GET_INT("port"));
  LOG_INFO("%30s: %10s", "Socket Family", GET_STRING("socket_family").c_str());
  LOG_INFO("%30s: %10s", "Statistics", GET_INT("stats_mode") ? "enabled" : "disabled");
  LOG_INFO("%30s: %10llu", "Max Connections", (unsigned long long)GET_INT("max_connections"));
  LOG_INFO("%30s: %10s", "Index Tuner", GET_BOOL("index_tuner") ? "enabled" : "disabled");
  LOG_INFO("%30s: %10s", "Layout Tuner", GET_BOOL("layout_tuner") ? "enabled" : "disabled");
  LOG_INFO("%30s: %10s", "Code-generation", GET_BOOL("codegen") ? "enabled" : "disabled");

  LOG_INFO(" ");
  LOG_INFO("%30s", "//===---------------------------------------------------===//");
}

void ConfigurationManager::Clear() {
  config.clear();
  catalog_initialized = false;
}

void init_parameters() {
  drop_parameters();
  register_parameters();
}

void drop_parameters() {
  auto config_manager = ConfigurationManager::GetInstance();
  config_manager->Clear();
  ::google::ShutDownCommandLineFlags();
}

}
}
