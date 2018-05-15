//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// manager_test.cpp
//
// Identification: test/catalog/manager_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "common/harness.h"

#include "catalog/manager.h"
#include "catalog/schema.h"
#include "common/macros.h"
#include "storage/layout.h"
#include "storage/tile_group.h"
#include "storage/tile_group_factory.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Manager Tests
//===--------------------------------------------------------------------===//

class ManagerTests : public PelotonTest {};

void AddTileGroup(UNUSED_ATTRIBUTE uint64_t thread_id) {

  // TILES
  std::vector<std::string> tile_column_names;
  std::vector<std::vector<std::string>> column_names;

  tile_column_names.push_back("INTEGER COL");
  column_names.push_back(tile_column_names);

  std::vector<catalog::Schema> schemas;
  std::vector<catalog::Column> columns;

  // SCHEMA
  catalog::Column column1(type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
                          "A", true);
  columns.push_back(column1);

  std::unique_ptr<catalog::Schema> schema1(new catalog::Schema(columns));
  schemas.push_back(*schema1);

  std::shared_ptr<const storage::Layout> layout =
      std::make_shared<const storage::Layout>(columns.size());

  for (oid_t txn_itr = 0; txn_itr < 100; txn_itr++) {
    std::unique_ptr<storage::TileGroup> tile_group(
        storage::TileGroupFactory::GetTileGroup(INVALID_OID, INVALID_OID,
                                                INVALID_OID, nullptr, schemas,
                                                layout, 3));
  }
}

TEST_F(ManagerTests, TransactionTest) {
  LaunchParallelTest(8, AddTileGroup);

  LOG_INFO("Catalog allocations :: %u",
           catalog::Manager::GetInstance().GetCurrentTileGroupId());

  // EXPECT_EQ(catalog::Manager::GetInstance().GetCurrentTileGroupId(), 800);
}

}  // namespace test
}  // namespace peloton
