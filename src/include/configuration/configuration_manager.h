//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// configuration_manager.h
//
// Identification: src/include/configuration/configuration_manager.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include "type/types.h"
#include "common/exception.h"
#include "common/printable.h"

namespace peloton {
namespace configuration {
  class Config;
}}
using peloton::configuration::Config;

namespace peloton {
namespace configuration {

class Config {
public:
  static uint64_t GET_INT(const std::string& name);
  static bool GET_BOOL(const std::string& name);
  static std::string GET_STRING(const std::string& name);

  static void SET_INT(const std::string &name, uint64_t value);
  static void SET_BOOL(const std::string &name, bool value);
  static void SET_STRING(const std::string &name, const std::string &value);

  static void init_parameters();
  static void drop_parameters();
};

class ConfigurationManager : public Printable {
public:
  static ConfigurationManager* GetInstance();

  void InitializeCatalog();

  const std::string GetInfo() const;

  void Clear();

  void Display();

  template<typename T>
  T GetValue(const std::string &name) {
    auto param = config.find(name);
    if (param == config.end()) {
      throw new ConfigurationException("no such configuration: " + name);
    }
    return to_value<T>(param->second.value);
  }

  template<typename T>
  void SetValue(const std::string &name, const T &value) {
    auto param = config.find(name);
    if (param == config.end()) {
      throw new ConfigurationException("no such configuration: " + name);
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
        throw new ConfigurationException("unsupported type");
    }
    if (catalog_initialized) {
      insert_into_catalog(param->first, param->second);
    }
  }

  template<typename T>
  void DefineConfig(const std::string &name, void* value, type::TypeId type,
                    const std::string &description, const T &default_value,
                    bool is_mutable, bool is_persistent) {
    if (config.count(name) > 0) {
      throw ConfigurationException("configuration " + name + " already exists");
    }
    T tmp(default_value);
    config[name] = Param(value, description, type, to_string(&tmp, type),
                         is_mutable, is_persistent);
    if (catalog_initialized) {
      insert_into_catalog(name, config[name]);
    }
  }

private:

  template<typename T>
  T to_value(void* value_p) {
    return *reinterpret_cast<T*>(value_p);
  }

  struct Param {
    void* value;
    std::string desc, default_value;
    type::TypeId value_type;
    bool is_mutable, is_persistent;
    Param() {}
    Param(void* val, std::string desc, type::TypeId type,
          std::string default_val, bool is_mutable, bool is_persistent)
            : value(val), desc(desc), default_value(default_val),
              value_type(type),
              is_mutable(is_mutable), is_persistent(is_persistent) {}
  };

  std::unordered_map<std::string, Param> config;
  std::unique_ptr<type::AbstractPool> pool_;
  bool catalog_initialized;

  ConfigurationManager();

  void insert_into_catalog(const std::string &name, const Param &param);

  std::string to_string(void* value_p, type::TypeId type);
};

} // End configuration namespace
} // End peloton namespace
