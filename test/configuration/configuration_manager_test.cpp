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
#include "configuration/configuration_manager.h"
#include "catalog/catalog.h"
#include "catalog/config_catalog.h"

namespace peloton {
namespace test {

class ConfigurationManagerTests : public PelotonTest {};

TEST_F(ConfigurationManagerTests, InitializationTest) {
  auto catalog = catalog::Catalog::GetInstance();
  catalog->Bootstrap();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto config_manager = configuration::ConfigurationManager::GetInstance();
  auto config_catalog = catalog::ConfigCatalog::GetInstance();

  Config::init_parameters();
  config_manager->InitializeCatalog();

  // test port (int)
  auto txn = txn_manager.BeginTransaction();
  uint64_t port = Config::GET_INT("port");
  uint64_t port_default = (uint64_t)atoll(config_catalog->GetDefaultValue("port", txn).c_str());
  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(port, port_default);

  // test socket_family (string)
  txn = txn_manager.BeginTransaction();
  std::string socket_family = Config::GET_STRING("socket_family");
  std::string socket_family_default = config_catalog->GetDefaultValue("socket_family", txn);
  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(socket_family, socket_family_default);

  // test index_tuner (bool)
  txn = txn_manager.BeginTransaction();
  bool index_tuner = Config::GET_BOOL("index_tuner");
  bool index_tuner_default = ("true" == config_catalog->GetDefaultValue("index_tuner", txn));
  txn_manager.CommitTransaction(txn);
  EXPECT_EQ(index_tuner, index_tuner_default);
}

TEST_F(ConfigurationManagerTests, ModificationTest) {
  auto catalog = catalog::Catalog::GetInstance();
  catalog->Bootstrap();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto config_manager = configuration::ConfigurationManager::GetInstance();
  auto config_catalog = catalog::ConfigCatalog::GetInstance();

  Config::init_parameters();
  config_manager->InitializeCatalog();

  // Define Parameter (int)
  uint64_t test_value_int = 233;
  config_manager->DefineConfig<uint64_t>("test_int", &test_value_int, type::TypeId::INTEGER,
                                         "for test", 12321, false, false);
  auto txn = txn_manager.BeginTransaction();
  uint64_t value1 = Config::GET_INT("test_int");
  uint64_t value2 = (uint64_t)(atoll(config_catalog->
                               GetConfigValue("test_int", txn).c_str()));
  EXPECT_EQ(test_value_int, value1);
  EXPECT_EQ(test_value_int, value2);
  txn_manager.CommitTransaction(txn);

  Config::SET_INT("test_int", 1234);

  txn = txn_manager.BeginTransaction();
  uint64_t value3 = Config::GET_INT("test_int");
  uint64_t value4 = (uint64_t)(atoll(config_catalog->
          GetConfigValue("test_int", txn).c_str()));
  EXPECT_EQ(test_value_int, 1234);
  EXPECT_EQ(test_value_int, value3);
  EXPECT_EQ(test_value_int, value4);
  txn_manager.CommitTransaction(txn);

  // Define Parameter (bool)
  bool test_value_bool = true;
  config_manager->DefineConfig<bool>("test_bool", &test_value_bool, type::TypeId::BOOLEAN,
                                     "for test", false, false, false);
  txn = txn_manager.BeginTransaction();
  bool value5 = Config::GET_BOOL("test_bool");
  bool value6 = ("true" == config_catalog->GetConfigValue("test_bool", txn));
  EXPECT_EQ(test_value_bool, value5);
  EXPECT_EQ(test_value_bool, value6);
  txn_manager.CommitTransaction(txn);

  Config::SET_BOOL("test_bool", false);

  txn = txn_manager.BeginTransaction();
  bool value7 = Config::GET_BOOL("test_bool");
  bool value8 = ("true" == config_catalog->GetConfigValue("test_bool", txn));
  EXPECT_EQ(test_value_bool, false);
  EXPECT_EQ(test_value_bool, value7);
  EXPECT_EQ(test_value_bool, value8);
  txn_manager.CommitTransaction(txn);

  // Define Parameter (string)
  std::string test_value_string = "abcdefg";
  config_manager->DefineConfig<std::string>("test_string", &test_value_string, type::TypeId::VARCHAR,
                                            "for test", "abc", false, false);
  txn = txn_manager.BeginTransaction();
  std::string value9 = Config::GET_STRING("test_string");
  std::string value10 = config_catalog->GetConfigValue("test_string", txn);
  EXPECT_EQ(test_value_string, value9);
  EXPECT_EQ(test_value_string, value10);
  txn_manager.CommitTransaction(txn);

  Config::SET_STRING("test_string", "qwer");

  txn = txn_manager.BeginTransaction();
  std::string value11 = Config::GET_STRING("test_string");
  std::string value12 = config_catalog->GetConfigValue("test_string", txn);
  EXPECT_EQ(test_value_string, "qwer");
  EXPECT_EQ(test_value_string, value11);
  EXPECT_EQ(test_value_string, value12);
  txn_manager.CommitTransaction(txn);
}

TEST_F(ConfigurationManagerTests, CocurrencyTest) {
  // TODO
}

}  // End test namespace
}  // End peloton namespace

