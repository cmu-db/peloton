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
#include "common/exception.h"
#include "catalog/config_catalog.h"
#include "concurrency/transaction_manager_factory.h"

uint64_t GET_INT(const std::string& name);
bool GET_BOOL(const std::string& name);
std::string GET_STRING(const std::string& name);

void SET_INT(const std::string &name, uint64_t value);
void SET_BOOL(const std::string &name, bool value);
void SET_STRING(const std::string &name, const std::string &value);

namespace peloton {
namespace configuration {

void init_parameters(int *argc = nullptr, char ***argv = nullptr);
void drop_parameters();

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

  void Clear();

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
  bool catalog_initialized;

  ConfigurationManager() {
    catalog_initialized = false;
    pool_.reset(new type::EphemeralPool());
  }

  void insert_into_catalog(const std::string &name, const Param &param) {
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

  template<typename T>
  T to_value(void* value_p) {
    return *reinterpret_cast<T*>(value_p);
  }

  std::string to_string(void* value_p, type::TypeId type) {
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
};

} // End configuration namespace
} // End peloton namespace