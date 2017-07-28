#include <gflags/gflags.h>

#include "type/value_factory.h"
#include "configuration/configuration_util.h"
#include "configuration/configuration_manager.h"

// gflags DECLARE
#define __CONFIG_GFLAGS_DECLARE__
#include "configuration/configuration_macro.h"
#include "configuration/configuration.h"
#undef __CONFIG_GFLAGS_DECLARE__

#define __CONFIG_DEFINE__
#include "configuration/configuration_macro.h"

namespace peloton {
namespace configuration {

int32_t ConfigurationUtil::GET_INT(ConfigurationId id) {
  return ConfigurationManager::GetInstance()->GetValue(id).GetAs<int32_t>();
}

bool ConfigurationUtil::GET_BOOL(ConfigurationId id) {
  return ConfigurationManager::GetInstance()->GetValue(id).GetAs<bool>();
}

std::string ConfigurationUtil::GET_STRING(ConfigurationId id) {
  return ConfigurationManager::GetInstance()->GetValue(id).GetAs<const char*>();
}

void ConfigurationUtil::SET_INT(ConfigurationId id, int32_t value) {
  ConfigurationManager::GetInstance()->SetValue(
      id, type::ValueFactory::GetIntegerValue(value));
}

void ConfigurationUtil::SET_BOOL(ConfigurationId id, bool value) {
  ConfigurationManager::GetInstance()->SetValue(
      id, type::ValueFactory::GetBooleanValue(value));
}

void ConfigurationUtil::SET_STRING(ConfigurationId id, const std::string &value) {
  ConfigurationManager::GetInstance()->SetValue(
      id, type::ValueFactory::GetVarcharValue(value));
}

} // End configuration namespace
} // End peloton namespace

#undef __CONFIG_DEFINE__
