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

void ConfigurationManager::insert_into_catalog(const std::string &name, const Param &param) {
  auto config_catalog = catalog::ConfigCatalog::GetInstance();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  type::AbstractPool *pool = pool_.get();
  config_catalog->DeleteConfig(name, txn);
  config_catalog->InsertConfig(name, to_string(param.value, param.value_type),
                               param.value_type, param.desc,
                               "", "", param.default_value,
                               param.is_mutable, param.is_persistent,
                               pool, txn);
  txn_manager.CommitTransaction(txn);
}

ConfigurationManager::ConfigurationManager() {
  catalog_initialized = false;
  pool_.reset(new type::EphemeralPool());
}

std::string ConfigurationManager::to_string(void* value_p, type::TypeId type) {
  switch (type) {
    case type::TypeId::INTEGER: {
      std::string s = "";
      uint64_t v = to_value<uint64_t>(value_p);
      while (v) {
        s = char('0' + (v % 10)) + s;
        v /= 10;
      }
      return s == "" ? "0" : s;
    }
    case type::TypeId::BOOLEAN:
      return to_value<bool>(value_p) ? "true" : "false";
    case type::TypeId::VARCHAR:
      return to_value<std::string>(value_p);
    default:
      throw new Exception("type is not supported in configuration");
  }
}

void init_parameters() {
  drop_parameters();
  register_parameters();
}

void drop_parameters() {
  auto config_manager = ConfigurationManager::GetInstance();
  config_manager->Clear();
}

}
}
