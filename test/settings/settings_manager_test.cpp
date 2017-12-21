//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// settings_manager_test.cpp
//
// Identification: test/settings/settings_manager_test.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "settings/settings_manager.h"
#include "catalog/catalog.h"
#include "catalog/settings_catalog.h"

namespace peloton {
namespace test {

class SettingsManagerTests : public PelotonTest {};

TEST_F(SettingsManagerTests, InitializationTest) {
  auto catalog = catalog::Catalog::GetInstance();
  catalog->Bootstrap();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto &config_manager = settings::SettingsManager::GetInstance();
  auto &settings_catalog = catalog::SettingsCatalog::GetInstance();

  config_manager.InitializeCatalog();

  // test port (int)
  auto txn = txn_manager.BeginTransaction();
  int32_t port = settings::SettingsManager::GetInt(settings::SettingId::port);
  int32_t port_default = atoi(settings_catalog.GetDefaultValue("port", txn).c_str());
  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(port, port_default);

  // test socket_family (string)
  txn = txn_manager.BeginTransaction();
  std::string socket_family = settings::SettingsManager::GetString(settings::SettingId::socket_family);
  std::string socket_family_default = settings_catalog.GetDefaultValue("socket_family", txn);
  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(socket_family, socket_family_default);

  // test index_tuner (bool)
  txn = txn_manager.BeginTransaction();
  bool index_tuner = settings::SettingsManager::GetBool(settings::SettingId::index_tuner);
  bool index_tuner_default = ("true" == settings_catalog.GetDefaultValue("index_tuner", txn));
  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(index_tuner, index_tuner_default);
}

TEST_F(SettingsManagerTests, ModificationTest) {
  auto catalog = catalog::Catalog::GetInstance();
  catalog->Bootstrap();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto &config_manager = settings::SettingsManager::GetInstance();
  auto &settings_catalog = catalog::SettingsCatalog::GetInstance();

  config_manager.InitializeCatalog();

  // modify int
  auto txn = txn_manager.BeginTransaction();
  int32_t value1 = settings::SettingsManager::GetInt(settings::SettingId::port);
  int32_t value2 = atoi(settings_catalog.GetSettingValue("port", txn).c_str());
  EXPECT_EQ(value1, value2);
  txn_manager.CommitTransaction(txn);

  settings::SettingsManager::SetInt(settings::SettingId::port, 12345);

  txn = txn_manager.BeginTransaction();
  int32_t value3 = settings::SettingsManager::GetInt(settings::SettingId::port);
  int32_t value4 = atoi(settings_catalog.GetSettingValue("port", txn).c_str());
  EXPECT_EQ(value3, 12345);
  EXPECT_EQ(value3, value4);
  txn_manager.CommitTransaction(txn);

  // modify bool
  txn = txn_manager.BeginTransaction();
  bool value5 = settings::SettingsManager::GetBool(settings::SettingId::index_tuner);
  bool value6 = ("true" == settings_catalog.GetSettingValue("index_tuner", txn));
  EXPECT_EQ(value5, value6);
  txn_manager.CommitTransaction(txn);

  settings::SettingsManager::SetBool(settings::SettingId::index_tuner, true);

  txn = txn_manager.BeginTransaction();
  bool value7 = settings::SettingsManager::GetBool(settings::SettingId::index_tuner);
  bool value8 = ("true" == settings_catalog.GetSettingValue("index_tuner", txn));
  EXPECT_TRUE(value7);
  EXPECT_EQ(value7, value8);
  txn_manager.CommitTransaction(txn);

  // modify string
  txn = txn_manager.BeginTransaction();
  std::string value9 = settings::SettingsManager::GetString(settings::SettingId::socket_family);
  std::string value10 = settings_catalog.GetSettingValue("socket_family", txn);
  EXPECT_EQ(value9, value10);
  txn_manager.CommitTransaction(txn);

  settings::SettingsManager::SetString(settings::SettingId::socket_family, "test");

  txn = txn_manager.BeginTransaction();
  std::string value11 = settings::SettingsManager::GetString(settings::SettingId::socket_family);
  std::string value12 = settings_catalog.GetSettingValue("socket_family", txn);
  EXPECT_EQ(value11, "test");
  EXPECT_EQ(value11, value12);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
