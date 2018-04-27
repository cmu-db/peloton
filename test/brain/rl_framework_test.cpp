//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rl_framework_test.cpp
//
// Identification: test/brain/rl_framework_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/index_selection.h"
#include "brain/indextune/compressed_index_config.h"
#include "catalog/catalog.h"
#include "catalog/database_catalog.h"
#include "catalog/index_catalog.h"
#include "catalog/table_catalog.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "util/file_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// RL Framework Tests
//===--------------------------------------------------------------------===//

class RLFrameworkTest : public PelotonTest {
 public:
  RLFrameworkTest(catalog::Catalog *cat = nullptr,
                  concurrency::TransactionManager *txn_manager = nullptr) {
    if (nullptr == cat) {
      cat = catalog::Catalog::GetInstance();
    }
    if (nullptr == txn_manager) {
      txn_manager = &concurrency::TransactionManagerFactory::GetInstance();
    }
    comp_idx_config_ = std::unique_ptr<brain::CompressedIndexConfiguration>(
        new brain::CompressedIndexConfiguration(cat, txn_manager));
  }

  std::unique_ptr<brain::CompressedIndexConfiguration> comp_idx_config_;
};

TEST_F(RLFrameworkTest, BasicTest) {
  std::string database_name = DEFAULT_DB_NAME;
  std::string table_name_1 = "dummy_table_1";
  std::string table_name_2 = "dummy_table_2";

  comp_idx_config_->CreateDatabase(database_name);
  comp_idx_config_->CreateTable(table_name_1);
  comp_idx_config_->CreateTable(table_name_2);

  // create index on (a, b) and (b, c)
  // (a, b) -> 110 -> 6 -> 6
  // (b, c) -> 011 -> 3 -> 3
  comp_idx_config_->CreateIndex_A(table_name_1);
  // create index on (a, c)
  // (a, c) -> 101 -> 5 -> 13
  comp_idx_config_->CreateIndex_B(table_name_2);

  auto cur_bit_set = comp_idx_config_->GenerateCurrentBitSet();
  std::string output;
  boost::to_string(*cur_bit_set, output);
  LOG_DEBUG("bitset: %s", output.c_str());
}

}  // namespace test
}  // namespace peloton
