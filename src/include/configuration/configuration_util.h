#pragma once

#include "configuration/configuration_type.h"

namespace peloton {
namespace configuration {

class ConfigurationUtil {
public:
  static int32_t GET_INT(ConfigurationId id);
  static bool GET_BOOL(ConfigurationId id);
  static std::string GET_STRING(ConfigurationId id);

  static void SET_INT(ConfigurationId id, int32_t value);
  static void SET_BOOL(ConfigurationId id, bool value);
  static void SET_STRING(ConfigurationId id, const std::string &value);
};

} // End configuration namespace
} // End peloton namespace

using namespace peloton::configuration;
