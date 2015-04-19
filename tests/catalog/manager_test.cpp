/*-------------------------------------------------------------------------
 *
 * catalog_test.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/tests/catalog/catalog_test.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "catalog/manager.h"
#include "gtest/gtest.h"

#include "harness.h"
#include "storage/tile_group.h"

namespace nstore {
namespace test {

//===--------------------------------------------------------------------===//
// Manager Tests
//===--------------------------------------------------------------------===//

void AddTileGroup(catalog::Manager *catalog){

  // TILES
  std::vector<std::string> tile_column_names;
  std::vector<std::vector<std::string> > column_names;

  tile_column_names.push_back("INTEGER COL");
  column_names.push_back(tile_column_names);

  std::vector<catalog::Schema> schemas;
  std::vector<catalog::ColumnInfo> columns;

  // SCHEMA
  catalog::ColumnInfo column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "A", false, true);
  columns.push_back(column1);

  catalog::Schema *schema1 = new catalog::Schema(columns);
  schemas.push_back(*schema1);

  storage::Backend *backend = new storage::VMBackend();

  for(id_t txn_itr = 0 ; txn_itr < 100 ; txn_itr++) {
    storage::TileGroup *tile_group = storage::TileGroupFactory::GetTileGroup(INVALID_OID, INVALID_OID, INVALID_OID,
                                                                             catalog, backend, schemas, 3);

    delete tile_group;
  }

  delete schema1;
  delete backend;
}


TEST(ManagerTests, TransactionTest) {

  catalog::Manager *catalog = new catalog::Manager();

  LaunchParallelTest(8, AddTileGroup, catalog);

  std::cout << "Catalog allocations :: " << catalog->GetOid() << "\n";

  EXPECT_EQ(catalog->GetOid(), 800);

  delete catalog;
}

} // End test namespace
} // End nstore namespace

