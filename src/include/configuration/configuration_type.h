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
