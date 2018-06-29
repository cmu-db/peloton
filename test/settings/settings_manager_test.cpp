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

#include "settings/settings_manager.h"
#include "catalog/catalog.h"
#include "catalog/settings_catalog.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"

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
  int32_t port = config_manager.GetInt(settings::SettingId::port);
  int32_t port_default =
      settings_catalog.GetSettingsCatalogEntry(txn, "port")
                      ->GetDefaultValue()
                      .GetAs<int32_t>();
  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(port, port_default);

  // test socket_family (string)
  txn = txn_manager.BeginTransaction();
  std::string socket_family =
      config_manager.GetString(settings::SettingId::socket_family);
  std::string socket_family_default =
      settings_catalog.GetSettingsCatalogEntry(txn, "socket_family")
                      ->GetDefaultValue().ToString();
  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(socket_family, socket_family_default);

  // test indextuner (bool)
  txn = txn_manager.BeginTransaction();
  bool index_tuner =
      config_manager.GetBool(settings::SettingId::index_tuner);
  bool index_tuner_default =
      settings_catalog.GetSettingsCatalogEntry(txn, "index_tuner")
                      ->GetDefaultValue().IsTrue();
  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(index_tuner, index_tuner_default);
}

TEST_F(SettingsManagerTests, ModificationTest) {
  // NOTE: Catalog::GetInstance()->Bootstrap() has been called in previous tests
  // you can only call it once!
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto &config_manager = settings::SettingsManager::GetInstance();
  auto &settings_catalog = catalog::SettingsCatalog::GetInstance();

  config_manager.InitializeCatalog();

  // modify int (value only)
  auto txn = txn_manager.BeginTransaction();
  int32_t value1 = config_manager.GetInt(settings::SettingId::port);
  int32_t value2 =
      settings_catalog.GetSettingsCatalogEntry(txn, "port")
                      ->GetValue()
                      .GetAs<int32_t>();
  EXPECT_EQ(value1, value2);
  txn_manager.CommitTransaction(txn);

  txn = txn_manager.BeginTransaction();
  config_manager.SetInt(settings::SettingId::port, 12345, false, txn);
  int32_t value3 = config_manager.GetInt(settings::SettingId::port);
  int32_t value4 =
      settings_catalog.GetSettingsCatalogEntry(txn, "port")
                      ->GetValue()
                      .GetAs<int32_t>();
  EXPECT_EQ(value3, 12345);
  EXPECT_EQ(value3, value4);
  txn_manager.CommitTransaction(txn);

  txn = txn_manager.BeginTransaction();
  config_manager.ResetValue(settings::SettingId::port, txn);
  int32_t value5 = config_manager.GetInt(settings::SettingId::port);
  int32_t value6 =
      settings_catalog.GetSettingsCatalogEntry(txn, "port")
                      ->GetValue()
                      .GetAs<int32_t>();
  EXPECT_EQ(value1, value5);
  EXPECT_EQ(value2, value6);
  txn_manager.CommitTransaction(txn);

  // modify bool (value only)
  txn = txn_manager.BeginTransaction();
  bool value7 = config_manager.GetBool(settings::SettingId::index_tuner);
  bool value8 =
      settings_catalog.GetSettingsCatalogEntry(txn, "index_tuner")
                      ->GetValue()
                      .IsTrue();
  EXPECT_EQ(value7, value8);
  txn_manager.CommitTransaction(txn);

  txn = txn_manager.BeginTransaction();
  config_manager.SetBool(settings::SettingId::index_tuner, true, false, txn);
  bool value9 = config_manager.GetBool(settings::SettingId::index_tuner);
  bool value10 =
      settings_catalog.GetSettingsCatalogEntry(txn, "index_tuner")
                      ->GetValue()
                      .IsTrue();
  EXPECT_TRUE(value9);
  EXPECT_EQ(value9, value10);
  txn_manager.CommitTransaction(txn);

  txn = txn_manager.BeginTransaction();
  config_manager.ResetValue(settings::SettingId::index_tuner, txn);
  bool value11 = config_manager.GetBool(settings::SettingId::index_tuner);
  bool value12 =
      settings_catalog.GetSettingsCatalogEntry(txn, "index_tuner")
                      ->GetValue()
                      .IsTrue();
  EXPECT_EQ(value7, value11);
  EXPECT_EQ(value8, value12);
  txn_manager.CommitTransaction(txn);

  // modify string (value + default value)
  txn = txn_manager.BeginTransaction();
  std::string value13 =
      config_manager.GetString(settings::SettingId::socket_family);
  std::string value14 =
      settings_catalog.GetSettingsCatalogEntry(txn, "socket_family")
                      ->GetValue()
                      .ToString();
  EXPECT_EQ(value13, value14);
  txn_manager.CommitTransaction(txn);

  txn = txn_manager.BeginTransaction();
  config_manager.SetString(settings::SettingId::socket_family, "test", true, txn);
  std::string value15 =
      config_manager.GetString(settings::SettingId::socket_family);
  std::string value16 =
      settings_catalog.GetSettingsCatalogEntry(txn, "socket_family")
                      ->GetValue()
                      .ToString();
  EXPECT_EQ(value15, "test");
  EXPECT_EQ(value16, value16);
  txn_manager.CommitTransaction(txn);

  txn = txn_manager.BeginTransaction();
  config_manager.ResetValue(settings::SettingId::index_tuner, txn);
  std::string value17 =
      config_manager.GetString(settings::SettingId::socket_family);
  std::string value18 =
      settings_catalog.GetSettingsCatalogEntry(txn, "socket_family")
                      ->GetValue()
                      .ToString();
  EXPECT_EQ(value15, value17);
  EXPECT_EQ(value16, value18);
  txn_manager.CommitTransaction(txn);
}

}  // namespace test
}  // namespace peloton
