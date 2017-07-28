//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// configuration_manager_test.cpp
//
// Identification: test/configuration/configuration_manager_test.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <include/catalog/catalog.h>
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "configuration/configuration_manager.h"
#include "configuration/configuration_util.h"
#include "catalog/catalog.h"
#include "catalog/settings_catalog.h"

namespace peloton {
namespace test {

class ConfigurationManagerTests : public PelotonTest {};

TEST_F(ConfigurationManagerTests, InitializationTest) {
  auto catalog = catalog::Catalog::GetInstance();
  catalog->Bootstrap();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto config_manager = configuration::ConfigurationManager::GetInstance();
  auto settings_catalog = catalog::SettingsCatalog::GetInstance();

  config_manager->InitializeCatalog();

  // test port (int)
  auto txn = txn_manager.BeginTransaction();
  uint64_t port = ConfigurationUtil::GET_INT(ConfigurationId::port);
  uint64_t port_default = (uint64_t)atoll(settings_catalog->GetDefaultValue("port", txn).c_str());
  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(port, port_default);

  // test socket_family (string)
  txn = txn_manager.BeginTransaction();
  std::string socket_family = ConfigurationUtil::GET_STRING(ConfigurationId::socket_family);
  std::string socket_family_default = settings_catalog->GetDefaultValue("socket_family", txn);
  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(socket_family, socket_family_default);

  // test index_tuner (bool)
  txn = txn_manager.BeginTransaction();
  bool index_tuner = ConfigurationUtil::GET_BOOL(ConfigurationId::index_tuner);
  bool index_tuner_default = ("true" == settings_catalog->GetDefaultValue("index_tuner", txn));
  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(index_tuner, index_tuner_default);
}

TEST_F(ConfigurationManagerTests, ModificationTest) {
  auto catalog = catalog::Catalog::GetInstance();
  catalog->Bootstrap();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto config_manager = configuration::ConfigurationManager::GetInstance();
  auto settings_catalog = catalog::SettingsCatalog::GetInstance();

  config_manager->InitializeCatalog();

  // modify int
  auto txn = txn_manager.BeginTransaction();
  uint64_t value1 = ConfigurationUtil::GET_INT(ConfigurationId::port);
  uint64_t value2 = (uint64_t)(atoll(settings_catalog->
                               GetSettingValue("port", txn).c_str()));
  EXPECT_EQ(value1, value2);
  txn_manager.CommitTransaction(txn);

  ConfigurationUtil::SET_INT(ConfigurationId::port, 12345);

  txn = txn_manager.BeginTransaction();
  uint64_t value3 = ConfigurationUtil::GET_INT(ConfigurationId::port);
  uint64_t value4 = (uint64_t)(atoll(settings_catalog->
          GetSettingValue("port", txn).c_str()));
  EXPECT_EQ(value3, 12345);
  EXPECT_EQ(value3, value4);
  txn_manager.CommitTransaction(txn);

  // modify bool
  txn = txn_manager.BeginTransaction();
  bool value5 = ConfigurationUtil::GET_BOOL(ConfigurationId::index_tuner);
  bool value6 = ("true" == settings_catalog->GetSettingValue("index_tuner", txn));
  EXPECT_EQ(value5, value6);
  txn_manager.CommitTransaction(txn);

  ConfigurationUtil::SET_BOOL(ConfigurationId::index_tuner, true);

  txn = txn_manager.BeginTransaction();
  bool value7 = ConfigurationUtil::GET_BOOL(ConfigurationId::index_tuner);
  bool value8 = ("true" == settings_catalog->GetSettingValue("index_tuner", txn));
  EXPECT_EQ(value7, true);
  EXPECT_EQ(value7, value8);
  txn_manager.CommitTransaction(txn);

  // modify string
  txn = txn_manager.BeginTransaction();
  std::string value9 = ConfigurationUtil::GET_STRING(ConfigurationId::socket_family);
  std::string value10 = settings_catalog->GetSettingValue("socket_family", txn);
  EXPECT_EQ(value9, value10);
  txn_manager.CommitTransaction(txn);

  ConfigurationUtil::SET_STRING(ConfigurationId::socket_family, "test");

  txn = txn_manager.BeginTransaction();
  std::string value11 = ConfigurationUtil::GET_STRING(ConfigurationId::socket_family);
  std::string value12 = settings_catalog->GetSettingValue("socket_family", txn);
  EXPECT_EQ(value11, "test");
  EXPECT_EQ(value11, value12);
  txn_manager.CommitTransaction(txn);
}

TEST_F(ConfigurationManagerTests, CocurrencyTest) {
  // TODO
}

}  // End test namespace
}  // End peloton namespace
