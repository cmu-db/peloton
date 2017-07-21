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

bool ConfigurationManager::GetBool(const std::string &name) {
  auto param = config.find(name);
  if (param == config.end()) {
    throw new Exception("no such configuration: " + name);
  }
  if (param->second.type != "bool") {
    throw new Exception("configuration " + name + " is not a bool");
  }
  return to_bool(param->second.value);
}

unsigned long long ConfigurationManager::GetInt(const std::string &name) {
  auto param = config.find(name);
  if (param == config.end()) {
    throw new Exception("no such configuration: " + name);
  }
  if (param->second.type != "int") {
    throw new Exception("configuration " + name + " is not a int");
  }
  return to_int(param->second.value);
}

std::string ConfigurationManager::GetString(const std::string &name) {
  auto param = config.find(name);
  if (param == config.end()) {
    throw new Exception("no such configuration: " + name);
  }
  return param->second.value;
}

void ConfigurationManager::SetBool(const std::string &name, bool value) {
  auto param = config.find(name);
  if (param == config.end()) {
    throw new Exception("no such configuration: " + name);
  }
  if (param->second.type != "bool") {
    throw new Exception("configuration " + name + " is not a bool");
  }
  return param->second.value = to_string(value);
}

void ConfigurationManager::SetInt(const std::string &name, unsigned long long value) {
  auto param = config.find(name);
  if (param == config.end()) {
    throw new Exception("no such configuration: " + name);
  }
  if (param->second.type != "int") {
    throw new Exception("configuration " + name + " is not a int");
  }
  return param->second.value = to_string(value);
}

void ConfigurationManager::SetString(const std::string &name, const std::string &value) {
  auto param = config.find(name);
  if (param == config.end()) {
    throw new Exception("no such configuration: " + name);
  }
  return param->second.value = value;
}

void ConfigurationManager::InitializeCatalog() {
  ConfigCatalog* config_catalog = ConfigCatalog::GetInstance();
  for (auto conf : config) {
  }
}

void ConfigurationManager::PrintConfiguration() {
  LOG_INFO("%30s", "//===-------------- PELOTON CONFIGURATION --------------===//");
  LOG_INFO(" ");

  LOG_INFO("%30s: %10llu", "Port", GetInt("port"));
  LOG_INFO("%30s: %10s", "Socket Family", GetString("socket_family").c_str());
  LOG_INFO("%30s: %10s", "Statistics", GetBool("stats_mode") ? "enabled" : "disabled");
  LOG_INFO("%30s: %10llu", "Max Connections", GetInt("max_connections"));
  LOG_INFO("%30s: %10s", "Index Tuner", GetBool("index_tuner") ? "enabled" : "disabled");
  LOG_INFO("%30s: %10s", "Layout Tuner", GetBool("layout_tuner") ? "enabled" : "disabled");
  LOG_INFO("%30s: %10s", "Code-generation", GetBool("codegen") ? "enabled" : "disabled");

  LOG_INFO(" ");
  LOG_INFO("%30s", "//===---------------------------------------------------===//");
}

bool ConfigurationManager::DefineBool(const std::string &name,
                                      bool value,
                                      const std::string &description,
                                      bool default_value,
                                      bool is_mutable,
                                      bool is_persistent) {
  if (config.count(name) > 0) {
    throw Exception("configuration " + name + " already exists");
  }
  config[name] = Param(to_string(value), description, "bool",
                       to_string(default_value), his_mutable, is_persistent);
}

bool ConfigurationManager::DefineInt(const std::string &name,
                                     unsigned long long value,
                                     const std::string &description,
                                     unsigned long long default_value,
                                     bool is_mutable,
                                     bool is_persistent) {
  if (config.count(name) > 0) {
    throw Exception("configuration " + name + " already exists");
  }
  config[name] = Param(to_string(value), description, "int",
                       to_string(default_value), his_mutable, is_persistent);
}

bool ConfigurationManager::DefineString(const std::string &name,
                                        const std::string &value,
                                        const std::string &description,
                                        const std::string default_value,
                                        bool is_mutable,
                                        bool is_persistent) {
  if (config.count(name) > 0) {
    throw Exception("configuration " + name + " already exists");
  }
  config[name] = Param(value, description, "string",
                       default_value, his_mutable, is_persistent);
}

}
}

