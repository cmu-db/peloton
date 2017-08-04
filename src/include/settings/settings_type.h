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

#define __SETTING_ENUM__
#include "settings/settings_macro.h"

namespace peloton {
namespace settings {

enum class SettingsId {
  #include "settings/settings.h"
};

}  // namespace settings
}  // namespace peloton

#undef __SETTING_ENUM__
