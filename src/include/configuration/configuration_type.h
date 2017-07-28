//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// configuration_type.h
//
// Identification: src/include/configuration/configuration_type.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#define __CONFIG_ENUM__
#include "configuration/configuration_macro.h"

namespace peloton {
namespace configuration {

enum class ConfigurationId {
  help = 0,
  #include "configuration/configuration.h"
};

} // End configuration namespace
} // End peloton namespace

#undef __CONFIG_ENUM__
