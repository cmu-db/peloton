//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// settings_macro.h
//
// Identification: src/include/settings/settings_macro.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#ifdef __SETTING_GFLAGS_DEFINE__
  #ifdef SETTING_int
    #undef SETTING_int
  #endif
  #ifdef SETTING_bool
    #undef SETTING_bool
  #endif
  #ifdef SETTING_string
    #undef SETTING_string
  #endif
  #define SETTING_int(name, description, default_value, is_mutable, is_persistent)     \
    DEFINE_int32(name, default_value, description);

  #define SETTING_bool(name, description, default_value, is_mutable, is_persistent)    \
    DEFINE_bool(name, default_value, description);

  #define SETTING_string(name, description, default_value, is_mutable, is_persistent)  \
    DEFINE_string(name, default_value, description);
#endif

#ifdef __SETTING_GFLAGS_DECLARE__
  #ifdef SETTING_int
    #undef SETTING_int
  #endif
  #ifdef SETTING_bool
    #undef SETTING_bool
  #endif
  #ifdef SETTING_string
    #undef SETTING_string
  #endif
  #define SETTING_int(name, description, default_value, is_mutable, is_persistent)     \
    DECLARE_int32(name);

  #define SETTING_bool(name, description, default_value, is_mutable, is_persistent)    \
    DECLARE_bool(name);

  #define SETTING_string(name, description, default_value, is_mutable, is_persistent)  \
    DECLARE_string(name);
#endif

#ifdef __SETTING_DEFINE__
  #ifdef SETTING_int
    #undef SETTING_int
  #endif
  #ifdef SETTING_bool
    #undef SETTING_bool
  #endif
  #ifdef SETTING_string
    #undef SETTING_string
  #endif
  #define SETTING_int(name, description, default_value, is_mutable, is_persistent)     \
      DefineSetting(                                                                   \
        peloton::settings::SettingsId::name,                                           \
        #name, type::ValueFactory::GetIntegerValue(FLAGS_##name),                      \
        description, type::ValueFactory::GetIntegerValue(default_value),               \
        is_mutable, is_persistent);

  #define SETTING_bool(name, description, default_value, is_mutable, is_persistent)    \
      DefineSetting(                                                                   \
        peloton::settings::SettingsId::name,                                           \
        #name, type::ValueFactory::GetBooleanValue(FLAGS_##name),                      \
        description, type::ValueFactory::GetBooleanValue(default_value),               \
        is_mutable, is_persistent);

  #define SETTING_string(name, description, default_value, is_mutable, is_persistent)  \
      DefineSetting(                                                                   \
        peloton::settings::SettingsId::name,                                           \
        #name, type::ValueFactory::GetVarcharValue(FLAGS_##name),                      \
        description, type::ValueFactory::GetVarcharValue(default_value),               \
        is_mutable, is_persistent);
#endif

#ifdef __SETTING_ENUM__
  #ifdef SETTING_int
    #undef SETTING_int
  #endif
  #ifdef SETTING_bool
    #undef SETTING_bool
  #endif
  #ifdef SETTING_string
    #undef SETTING_string
  #endif
  #define SETTING_int(name, description, default_value, is_mutable, is_persistent)     \
    name,

  #define SETTING_bool(name, description, default_value, is_mutable, is_persistent)    \
    name,

  #define SETTING_string(name, description, default_value, is_mutable, is_persistent)  \
    name,
#endif
