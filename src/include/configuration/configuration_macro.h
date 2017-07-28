//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// configuration_util.h
//
// Identification: src/include/configuration/configuration_util.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#ifdef __CONFIG_GFLAGS_DEFINE__
  #ifdef CONFIG_int
    #undef CONFIG_int
  #endif
  #ifdef CONFIG_bool
    #undef CONFIG_bool
  #endif
  #ifdef CONFIG_string
    #undef CONFIG_string
  #endif
  #define CONFIG_int(name, description, default_value, is_mutable, is_persistent)     \
    DEFINE_int32(name, default_value, description);

  #define CONFIG_bool(name, description, default_value, is_mutable, is_persistent)    \
    DEFINE_bool(name, default_value, description);

  #define CONFIG_string(name, description, default_value, is_mutable, is_persistent)  \
    DEFINE_string(name, default_value, description);
#endif

#ifdef __CONFIG_GFLAGS_DECLARE__
  #ifdef CONFIG_int
    #undef CONFIG_int
  #endif
  #ifdef CONFIG_bool
    #undef CONFIG_bool
  #endif
  #ifdef CONFIG_string
    #undef CONFIG_string
  #endif
  #define CONFIG_int(name, description, default_value, is_mutable, is_persistent)     \
    DECLARE_int32(name);

  #define CONFIG_bool(name, description, default_value, is_mutable, is_persistent)    \
    DECLARE_bool(name);

  #define CONFIG_string(name, description, default_value, is_mutable, is_persistent)  \
    DECLARE_string(name);
#endif

#ifdef __CONFIG_DEFINE__
  #ifdef CONFIG_int
    #undef CONFIG_int
  #endif
  #ifdef CONFIG_bool
    #undef CONFIG_bool
  #endif
  #ifdef CONFIG_string
    #undef CONFIG_string
  #endif
  #define CONFIG_int(name, description, default_value, is_mutable, is_persistent)     \
      DefineConfig(                                                      \
        peloton::configuration::ConfigurationId::name,                            \
        #name, &FLAGS_##name, description,                         \
        type::ValueFactory::GetIntegerValue(FLAGS_##name), \
        type::ValueFactory::GetIntegerValue(default_value),\
        is_mutable, is_persistent);

  #define CONFIG_bool(name, description, default_value, is_mutable, is_persistent)    \
      DefineConfig(                                                      \
        peloton::configuration::ConfigurationId::name,                            \
        #name, &FLAGS_##name, description,                         \
        type::ValueFactory::GetBooleanValue(FLAGS_##name), \
        type::ValueFactory::GetBooleanValue(default_value),\
        is_mutable, is_persistent);

  #define CONFIG_string(name, description, default_value, is_mutable, is_persistent)  \
      DefineConfig(                                                      \
        peloton::configuration::ConfigurationId::name,                            \
        #name, &FLAGS_##name, description,                         \
        type::ValueFactory::GetVarcharValue(FLAGS_##name), \
        type::ValueFactory::GetVarcharValue(default_value),\
        is_mutable, is_persistent);
#endif

#ifdef __CONFIG_ENUM__
  #ifdef CONFIG_int
    #undef CONFIG_int
  #endif
  #ifdef CONFIG_bool
    #undef CONFIG_bool
  #endif
  #ifdef CONFIG_string
    #undef CONFIG_string
  #endif
  #define CONFIG_int(name, description, default_value, is_mutable, is_persistent)     \
    name,

  #define CONFIG_bool(name, description, default_value, is_mutable, is_persistent)    \
    name,

  #define CONFIG_string(name, description, default_value, is_mutable, is_persistent)  \
    name,
#endif
