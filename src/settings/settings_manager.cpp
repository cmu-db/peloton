//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// settings_manager.cpp
//
// Identification: src/settings/settings_manager.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <gflags/gflags.h>

#include "type/type_id.h"
#include "catalog/settings_catalog.h"
#include "common/exception.h"
#include "concurrency/transaction_manager_factory.h"
#include "settings/settings_manager.h"
#include "type/ephemeral_pool.h"
#include "type/value_factory.h"
#include "util/stringbox_util.h"

// This will expand to define all the settings defined in settings.h
// using GFlag's DEFINE_...() macro. See settings_macro.h.
#define __SETTING_GFLAGS_DEFINE__
#include "settings/settings_macro.h"
#include "settings/settings.h"
#undef __SETTING_GFLAGS_DEFINE__

namespace peloton {
namespace settings {

SettingsManager &SettingsManager::GetInstance() {
  static SettingsManager settings_manager;
  return settings_manager;
}

/** @brief      Get setting value as integer.
 *  @param      ID to get the setting.
 *  @return     Setting value
 */
int32_t SettingsManager::GetInt(SettingId id) const {
  return GetValue(id).GetAs<int32_t>();
}

/** @brief      Get setting value as double.
 *  @param      ID to get the setting.
 *  @return     Setting value
 */
double SettingsManager::GetDouble(SettingId id) const {
  return GetValue(id).GetAs<double>();
}

/** @brief      Get setting value as boolean.
 *  @param      ID to get the setting.
 *  @return     Setting value
 */
bool SettingsManager::GetBool(SettingId id) const {
  return GetValue(id).GetAs<bool>();
}

/** @brief      Get setting value as string.
 *  @param      ID to get the setting.
 *  @return     Setting value
 */
std::string SettingsManager::GetString(SettingId id) const {
  return GetValue(id).ToString();
}

/** @brief      Get setting value as Value object.
 *  @param      ID to get the setting.
 *  @return     Setting value
 */
type::Value SettingsManager::GetValue(SettingId id) const {
  // TODO: Look up the value from catalog
  // Because querying a catalog table needs to create a new transaction and
  // creating transaction needs to get setting values,
  // it will be a infinite recursion here.

  auto param = settings_.find(id);
  return param->second.value;
}

/** @brief      Set setting value as integer.
 *  @param      id  ID to set the setting.
 *  @param      value  Value set to the setting.
 *  @param      set_default  Set the value to default in addition if true.
 *  @param      txn  TransactionContext for the catalog control.
 *                   This has to be set after catalog initialization.
 */
void SettingsManager::SetInt(SettingId id, int32_t value, bool set_default,
                             concurrency::TransactionContext *txn) {
  SetValue(id, type::ValueFactory::GetIntegerValue(value), set_default, txn);
}

/** @brief      Set setting value as double.
 *  @param      id  ID to set the setting.
 *  @param      value  Value set to the setting.
 *  @param      set_default  Set the value to default in addition if true.
 *  @param      txn  TransactionContext for the catalog control.
 *                   This has to be set after catalog initialization.
 */
void SettingsManager::SetDouble(SettingId id, double value, bool set_default,
                                concurrency::TransactionContext *txn) {
  SetValue(id, type::ValueFactory::GetDecimalValue(value), set_default, txn);
}

/** @brief      Set setting value as boolean.
 *  @param      id  ID to set the setting.
 *  @param      value  Value set to the setting.
 *  @param      set_default  Set the value to default in addition if true.
 *  @param      txn  TransactionContext for the catalog control.
 *                   This has to be set after catalog initialization.
 */
void SettingsManager::SetBool(SettingId id, bool value, bool set_default,
                              concurrency::TransactionContext *txn) {
  SetValue(id, type::ValueFactory::GetBooleanValue(value), set_default, txn);
}

/** @brief      Set setting value as string.
 *  @param      id  ID to set the setting.
 *  @param      value  Value set to the setting.
 *  @param      set_default  Set the value to default in addition if true.
 *  @param      txn  TransactionContext for the catalog control.
 *                   This has to be set after catalog initialization.
 */
void SettingsManager::SetString(SettingId id, const std::string &value,
                                bool set_default,
                                concurrency::TransactionContext *txn) {
  SetValue(id, type::ValueFactory::GetVarcharValue(value), set_default, txn);
}

/** @brief      Set setting value as Value object.
 *  @param      id  ID to set the setting.
 *  @param      value  Value set to the setting.
 *  @param      set_default  Set the value to default in addition if true.
 *  @param      txn  TransactionContext for the catalog control.
 *                   This has to be set after catalog initialization.
 */
void SettingsManager::SetValue(SettingId id, const type::Value &value,
                               bool set_default,
                               concurrency::TransactionContext *txn) {
  auto param = settings_.find(id);

  // Check min-max bound if value is a following type
  if ((value.GetTypeId() == type::TypeId::BIGINT ||
       value.GetTypeId() == type::TypeId::INTEGER ||
       value.GetTypeId() == type::TypeId::SMALLINT ||
       value.GetTypeId() == type::TypeId::TINYINT ||
       value.GetTypeId() == type::TypeId::DECIMAL) &&
      !value.CompareBetweenInclusive(param->second.min_value,
                                     param->second.max_value)) {
    throw SettingsException("Value given for \"" + param->second.name +
                            "\" is not in its min-max bounds (" +
                            param->second.min_value.ToString() + "-" +
                            param->second.max_value.ToString() + ")");
  }

  // Catalog update if initialized
  if (catalog_initialized_) {
    PELOTON_ASSERT(txn != nullptr);
    if (!UpdateCatalog(param->second.name, value, set_default, txn)) {
      throw SettingsException("failed to set value " + value.ToString() +
                              " to " + param->second.name);
    }
  }

  // Set value
  param->second.value = value;

  // Set default_value if set_default is true
  if (set_default) {
    param->second.default_value = value;
  }
}

/** @brief      Reset a setting value to default value.
 *  @param      id  ID to reset the setting.
 *  @param      txn  TransactionContext for the catalog control.
 *                   This has to be set after catalog initialization.
 */
void SettingsManager::ResetValue(SettingId id,
                                 concurrency::TransactionContext *txn) {
  auto param = settings_.find(id);
  auto default_value = param->second.default_value;

  // Catalog update if initialized
  if (catalog_initialized_) {
    PELOTON_ASSERT(txn != nullptr);
    if (!UpdateCatalog(param->second.name, default_value, false, txn)) {
      throw SettingsException("failed to set value " +
                              default_value.ToString() + " to " +
                              param->second.name);
    }
  }

  // set default_value into the setting value
  param->second.value = default_value;
}

void SettingsManager::InitializeCatalog() {
  if (catalog_initialized_) return;

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  for (auto s : settings_) {
    if (!InsertCatalog(s.second, txn)) {
      txn_manager.AbortTransaction(txn);
      throw SettingsException("failed to initialize catalog pg_settings on " +
                              s.second.name);
    }
  }
  txn_manager.CommitTransaction(txn);
  catalog_initialized_ = true;
}

/** @brief      Refresh all setting values from setting catalog.
 *  @param      txn  TransactionContext for the catalog control.
 */
void SettingsManager::UpdateSettingListFromCatalog(
    concurrency::TransactionContext *txn) {
  auto &settings_catalog = catalog::SettingsCatalog::GetInstance(txn);

  // Update each catalog value from pg_settings
  for (auto setting_entry_pair : settings_catalog.GetSettings(txn)) {
    auto setting_name = setting_entry_pair.first;
    auto setting_entry = setting_entry_pair.second;

    // Find the setting from setting list on memory
    auto param = settings_.begin();
    while (param != settings_.end()) {
      if (param->second.name == setting_name) {
        break;
      }
      param++;
    }
    PELOTON_ASSERT(param != settings_.end());

    // Set value and default_value
    param->second.value = setting_entry->GetValue();
    param->second.default_value = setting_entry->GetDefaultValue();
  }
}

const std::string SettingsManager::GetInfo() const {
  const uint32_t box_width = 72;
  const std::string title = "PELOTON SETTINGS";

  std::string info;
  info.append(StringUtil::Format("%*s\n", box_width / 2 + title.length() / 2,
                                 title.c_str()));
  info.append(StringUtil::Repeat("=", box_width)).append("\n");

  // clang-format off
  info.append(StringUtil::Format("%34s:   %-34i\n", "Port", GetInt(SettingId::port)));
  info.append(StringUtil::Format("%34s:   %-34s\n", "Socket Family", GetString(SettingId::socket_family).c_str()));
  info.append(StringUtil::Format("%34s:   %-34s\n", "Statistics", GetInt(SettingId::stats_mode) ? "enabled" : "disabled"));
  info.append(StringUtil::Format("%34s:   %-34i\n", "Max Connections", GetInt(SettingId::max_connections)));
  info.append(StringUtil::Format("%34s:   %-34s\n", "Index Tuner", GetBool(SettingId::index_tuner) ? "enabled" : "disabled"));
  info.append(StringUtil::Format("%34s:   %-34s\n", "Layout Tuner", GetBool(SettingId::layout_tuner) ? "enabled" : "disabled"));
  info.append(StringUtil::Format("%34s:   (queue size %i, %i threads)\n", "Worker Pool", GetInt(SettingId::monoqueue_task_queue_size), GetInt(SettingId::monoqueue_worker_pool_size)));
  info.append(StringUtil::Format("%34s:   %-34s\n", "Parallel Query Execution", GetBool(SettingId::parallel_execution) ? "enabled" : "disabled"));
  info.append(StringUtil::Format("%34s:   %-34i\n", "Min. Parallel Table Scan Size", GetInt(SettingId::min_parallel_table_scan_size)));
  info.append(StringUtil::Format("%34s:   %-34s\n", "Code-generation", GetBool(SettingId::codegen) ? "enabled" : "disabled"));
  info.append(StringUtil::Format("%34s:   %-34s\n", "Print IR Statistics", GetBool(SettingId::print_ir_stats) ? "enabled" : "disabled"));
  info.append(StringUtil::Format("%34s:   %-34s\n", "Dump IR", GetBool(SettingId::dump_ir) ? "enabled" : "disabled"));
  info.append(StringUtil::Format("%34s:   %-34i\n", "Optimization Timeout", GetInt(SettingId::task_execution_timeout)));
  info.append(StringUtil::Format("%34s:   %-34i\n", "Number of GC threads", GetInt(SettingId::gc_num_threads)));
  // clang-format on

  return StringBoxUtil::Box(info);
}

void SettingsManager::ShowInfo() { LOG_INFO("\n%s\n", GetInfo().c_str()); }

void SettingsManager::DefineSetting(SettingId id, const std::string &name,
                                    const type::Value &value,
                                    const std::string &description,
                                    const type::Value &default_value,
                                    const type::Value &min_value,
                                    const type::Value &max_value,
                                    bool is_mutable, bool is_persistent) {
  if (settings_.find(id) != settings_.end()) {
    throw SettingsException("settings " + name + " already exists");
  }

  // Only below types support min-max bound checking
  if (value.GetTypeId() == type::TypeId::BIGINT ||
      value.GetTypeId() == type::TypeId::INTEGER ||
      value.GetTypeId() == type::TypeId::SMALLINT ||
      value.GetTypeId() == type::TypeId::TINYINT ||
      value.GetTypeId() == type::TypeId::DECIMAL) {
    if (!value.CompareBetweenInclusive(min_value, max_value))
      throw SettingsException(
          "Value given for \"" + name + "\" is not in its min-max bounds (" +
          min_value.ToString() + "-" + max_value.ToString() + ")");
  }

  settings_.emplace(id, Param(name, value, description, default_value,
                              min_value, max_value, is_mutable, is_persistent));
}

/** @brief      Insert a setting information into setting catalog.
 *  @param      param  Setting information.
 *  @param      txn  TransactionContext for the catalog control.
 *  @return     True if success, or false
 */
bool SettingsManager::InsertCatalog(const Param &param,
                                    concurrency::TransactionContext *txn) {
  auto &settings_catalog = catalog::SettingsCatalog::GetInstance();

  // Check a same setting is not existed
  if (settings_catalog.GetSetting(param.name, txn) != nullptr) {
    LOG_ERROR("The setting %s is already existed", param.name.c_str());
    return false;
  }

  if (!settings_catalog.InsertSetting(
          param.name, param.value.ToString(), param.value.GetTypeId(),
          param.desc, param.min_value.ToString(), param.max_value.ToString(),
          param.default_value.ToString(), param.is_mutable, param.is_persistent,
          pool_.get(), txn)) {
    return false;
  }
  return true;
}

/** @brief      Update a setting value within setting catalog.
 *  @param      name  Setting name to be updated.
 *  @param      value  Setting value to be set as a new value.
 *  @param      set_default  a flag whether update the default value.
 *  @param      txn  TransactionContext for the catalog control.
 *  @return     True if success, or false
 */
bool SettingsManager::UpdateCatalog(const std::string &name,
                                    const type::Value &value, bool set_default,
                                    concurrency::TransactionContext *txn) {
  auto &settings_catalog = catalog::SettingsCatalog::GetInstance();
  if (!settings_catalog.UpdateSettingValue(txn, name, value.ToString(),
                                           set_default)) {
    return false;
  }
  return true;
}

SettingsManager::SettingsManager() {
  catalog_initialized_ = false;
  pool_.reset(new type::EphemeralPool());

// This will expand to invoke SettingsManager::DefineSetting on
// all of the settings defined in settings.h. See settings_macro.h.
#define __SETTING_DEFINE__
#include "settings/settings_macro.h"
#include "settings/settings.h"
#undef __SETTING_DEFINE__
}

}  // namespace settings
}  // namespace peloton
