//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// configuration_manager.h
//
// Identification: src/include/configuration/configuration_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <gflags/gflags.h>
#include "type/types.h"
#include "type/ephemeral_pool.h"

#define GET_INT(name) (peloton::configuration::ConfigurationManager::GetInstance()->GetValue<unsigned long long>(name))
#define GET_BOOL(name) (peloton::configuration::ConfigurationManager::GetInstance()->GetValue<bool>(name))
#define GET_STRING(name) (peloton::configuration::ConfigurationManager::GetInstance()->GetValue<std::string>(name))

#define SET_INT(name, value) (peloton::configuration::ConfigurationManager::GetInstance()->SetValue<unsigned long long>(name, value))
#define SET_BOOL(name, value) (peloton::configuration::ConfigurationManager::GetInstance()->SetValue<bool>(name, value))
#define SET_STRING(name, value) (peloton::configuration::ConfigurationManager::GetInstance()->SetValue<std::string>(name, value))

namespace peloton {
namespace configuration {

void initialize_parameters();

class ConfigurationManager {
public:
  static ConfigurationManager* GetInstance();

  template<typename T>
  T GetValue(const std::string &name);

  template<typename T>
  void SetValue(const std::string &name, const T &value);

  template<typename T>
  void DefineConfig(const std::string &name, void* value, type::TypeId type,
                    const std::string &description, const T &default_value,
                    bool is_mutable, bool is_persistent);

  void InitializeCatalog();

  void PrintConfiguration();

private:

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

  ConfigurationManager() {
    pool_.reset(new type::EphemeralPool());
  }

  template<typename T>
  T to_value(const std::string &s, type::TypeId type) {
    switch (type) {
      case type::TypeId::INTEGER:
        return reinterpret_cast<T>(atoll(s.c_str()));
      case type::TypeId::BOOLEAN:
        return reinterpret_cast<T>(s == "true");
      case type::TypeId::VARCHAR:
        return reinterpret_cast<T>(s);
    }
    throw new Exception("type " + type + " is not supported in configuration");
  }

  template<typename T>
  std::string to_string(const T &value, type::TypeId type) {
    std::string s = "";
    int v;
    switch (type) {
      case type::TypeId::INTEGER:
        v = reinterpret_cast<unsigned long long>(value);
        while (v) {
          s = char('0' + (v % 10)) + s;
          v /= 10;
        }
        return s == "" ? "0" : s;
      case type::TypeId::BOOLEAN:
        return reinterpret_cast<bool>(value) ? "true" : "false";
      case type::TypeId::VARCHAR:
        return reinterpret_cast<std::string>(value);
    }
    throw new Exception("type " + type + " is not supported in configuration");
  }
};

} // End configuration namespace
} // End peloton namespace