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

#include <map>
#include "type/types.h"
#include "type/value.h"
#include "common/exception.h"
#include "common/printable.h"
#include "configuration/configuration_type.h"

namespace peloton {
namespace configuration {

class ConfigurationManager : public Printable {

  friend class ConfigurationUtil;

public:
  static ConfigurationManager* GetInstance();

  void InitializeCatalog();

  const std::string GetInfo() const;

  void ShowInfo();

private:

  struct Param {
    std::string name;
    void* gflags_ptr;
    std::string desc;
    type::Value value, default_value;
    bool is_mutable, is_persistent;
    Param() {}
    Param(std::string name, void* gflags_ptr, std::string desc,
          const type::Value &value, const type::Value &default_value,
          bool is_mutable, bool is_persistent)
            : name(name), gflags_ptr(gflags_ptr), desc(desc),
              value(value), default_value(default_value),
              is_mutable(is_mutable), is_persistent(is_persistent) {}
  };

  std::map<ConfigurationId, Param> config;

  std::unique_ptr<type::AbstractPool> pool_;

  bool catalog_initialized;

  ConfigurationManager();

  void DefineConfig(ConfigurationId id, const std::string &name,
                    void* gflags_ptr, const std::string &description,
                    const type::Value &value, const type::Value &default_value,
                    bool is_mutable, bool is_persistent);

  type::Value GetValue(ConfigurationId id);

  void SetValue(ConfigurationId id, const type::Value &value);

  void insert_into_catalog(const Param &param);
};

} // End configuration namespace
} // End peloton namespace
