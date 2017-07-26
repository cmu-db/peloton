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
#define __PELOTON_CONFIG__

#include "common/exception.h"
#include "configuration/configuration_manager.h"
#include "configuration/configuration.h"

namespace peloton {
namespace configuration {

uint64_t Config::GET_INT(const std::string& name) {
  return peloton::configuration::ConfigurationManager::GetInstance()->GetValue<uint64_t>(name);
}

bool Config::GET_BOOL(const std::string& name) {
  return peloton::configuration::ConfigurationManager::GetInstance()->GetValue<bool>(name);
}

std::string Config::GET_STRING(const std::string& name) {
  return peloton::configuration::ConfigurationManager::GetInstance()->GetValue<std::string>(name);
}

void Config::SET_INT(const std::string &name, uint64_t value) {
  peloton::configuration::ConfigurationManager::GetInstance()->SetValue<uint64_t>(name, value);
}

void Config::SET_BOOL(const std::string &name, bool value) {
  peloton::configuration::ConfigurationManager::GetInstance()->SetValue<bool>(name, value);
}

void Config::SET_STRING(const std::string &name, const std::string &value) {
  peloton::configuration::ConfigurationManager::GetInstance()->SetValue<std::string>(name, value);
}

void Config::init_parameters() {
  drop_parameters();
  register_parameters();
}

void Config::drop_parameters() {
  auto config_manager = ConfigurationManager::GetInstance();
  config_manager->Clear();
}

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

const std::string ConfigurationManager::GetInfo() const {
  std::stringstream str;
  char buffer[256];
  sprintf(buffer, "%30s", "//===-------------- PELOTON CONFIGURATION --------------===//");
  str << buffer << std::endl;
  sprintf(buffer, " ");
  str << buffer << std::endl;

  sprintf(buffer, "%30s: %10llu", "Port", (unsigned long long)Config::GET_INT("port"));
  str << buffer << std::endl;
  sprintf(buffer, "%30s: %10s", "Socket Family", Config::GET_STRING("socket_family").c_str());
  str << buffer << std::endl;
  sprintf(buffer, "%30s: %10s", "Statistics", Config::GET_INT("stats_mode") ? "enabled" : "disabled");
  str << buffer << std::endl;
  sprintf(buffer, "%30s: %10llu", "Max Connections", (unsigned long long)Config::GET_INT("max_connections"));
  str << buffer << std::endl;
  sprintf(buffer, "%30s: %10s", "Index Tuner", Config::GET_BOOL("index_tuner") ? "enabled" : "disabled");
  str << buffer << std::endl;
  sprintf(buffer, "%30s: %10s", "Layout Tuner", Config::GET_BOOL("layout_tuner") ? "enabled" : "disabled");
  str << buffer << std::endl;
  sprintf(buffer, "%30s: %10s", "Code-generation", Config::GET_BOOL("codegen") ? "enabled" : "disabled");
  str << buffer << std::endl;

  sprintf(buffer, " ");
  str << buffer << std::endl;
  sprintf(buffer, "%30s", "//===---------------------------------------------------===//");
  str << buffer << std::endl;

  return str.str();
}

void ConfigurationManager::Display() {
  LOG_INFO("%s", GetInfo().c_str());
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
      throw new ConfigurationException("type is not supported in configuration");
  }
}

} // End configuration namespace
} // End peloton namespace

