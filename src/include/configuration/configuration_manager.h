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

#include "type/types.h"
#include <gflags/gflags.h>

namespace peloton {
namespace configuration {

void initialize_configuration_manager();

class ConfigurationManager {
public:
  static ConfigurationManager* GetInstance();

  bool GetBool(const std::string &name);

  unsigned long long GetInt(const std::string &name);

  std::string GetString(const std::string &name);

  bool SetBool(const std::string &name, bool value);

  bool SetInt(const std::string &name, unsigned long long value);

  bool SetString(const std::string &name, const std::string &value);

  void InitializeCatalog();

  void PrintConfiguration();

private:

  struct Param {
    std::string value, desc, type;
    std::string default_value;
    bool is_mutable, is_persistent;
    Param(std::string val, std::string desc, std::string type,
          std::string default_val, bool is_mutable, bool is_persistent)
            : value(val), desc(desc), type(type),
              default_value(default_val),
              is_mutable(is_mutable), is_persistent(is_persistent) {}
  };

  std::map<std::string, Param> config;

  bool is_null(const std::string &s) {
    return s == "";
  }

  bool to_bool(const std::string &s) {
    return s == "true";
  }

  unsigned long long to_int(const std::string &s) {
    return atoll(s.data());
  }

  std::string to_string(bool value) {
    return value ? "true" : "false";
  }

  std::string to_string(unsigned long long value) {
    string n = "";
    while (value) {
      n += '0' + (value % 10);
      value /= 10;
    }
    return n == "" ? "0" : n;
  }

public:

  bool DefineBool(const std::string &name,
                  bool value,
                  const std::string &description,
                  bool default_value,
                  bool is_mutable,
                  bool is_persistent);

  bool DefineInt(const std::string &name,
                 unsigned long long value,
                 const std::string &description,
                 unsigned long long default_value,
                 bool is_mutable,
                 bool is_persistent);

  bool DefineString(const std::string &name,
                    const std::string &value,
                    const std::string &description,
                    const std::string default_value,
                    bool is_mutable,
                    bool is_persistent);
};

} // End configuration namespace
} // End peloton namespace