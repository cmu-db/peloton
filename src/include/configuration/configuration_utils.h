//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// configuration_utils.h
//
// Identification: src/include/configuration/configuration_utils.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

// internal use only in
// configuration/configuration_manager.cpp
#ifdef __PELOTON_CONFIG__

#include <gflags/gflags.h>

#define CONFIG_int(name, description, default_value, is_mutable, is_persistent)                   \
  DEFINE_uint64(name, default_value, description);                                                \
  void __register_##name() {                                                                      \
    auto config = peloton::configuration::ConfigurationManager::GetInstance();                    \
    config->DefineConfig<uint64_t>(#name, &FLAGS_##name, peloton::type::TypeId::INTEGER,          \
                                   description, default_value, is_mutable, is_persistent);        \
  }

#define CONFIG_bool(name, description, default_value, is_mutable, is_persistent)                  \
  DEFINE_bool(name, default_value, description);                                                  \
  void __register_##name() {                                                                      \
    auto config = peloton::configuration::ConfigurationManager::GetInstance();                    \
    config->DefineConfig<bool>(#name, &FLAGS_##name, peloton::type::TypeId::BOOLEAN,              \
                               description, default_value, is_mutable, is_persistent);            \
  }

#define CONFIG_string(name, description, default_value, is_mutable, is_persistent)                \
  DEFINE_string(name, default_value, description);                                                \
  void __register_##name() {                                                                      \
    auto config = peloton::configuration::ConfigurationManager::GetInstance();                    \
    config->DefineConfig<std::string>(#name, &FLAGS_##name, peloton::type::TypeId::VARCHAR,       \
                                      description, default_value, is_mutable, is_persistent);     \
  }

#define REGISTER(name) __register_##name();

#endif
