//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// settings_type.h
//
// Identification: src/include/settings/settings_type.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

namespace peloton {
namespace settings {

enum class SettingId {
  #define __SETTING_ENUM__
  #include "settings/settings_macro.h"
  #include "settings/settings.h"
  #undef __SETTING_ENUM__
};

}  // namespace settings
}  // namespace peloton

