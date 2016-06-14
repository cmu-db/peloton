//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// catalog_test.cpp
//
// Identification: test/catalog/catalog_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>

#include "gtest/gtest.h"
#include "common/harness.h"
#include "common/logger.h"
#include "catalog/bootstrapper.h"
#include "catalog/catalog.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Catalog Tests
//===--------------------------------------------------------------------===//

class CatalogTests : public PelotonTest {};

auto &bootstrapper = catalog::Bootstrapper::GetInstance();
auto global_catalog = bootstrapper.bootstrap();
auto &bootstrapper2 = catalog::Bootstrapper::GetInstance();
auto global_catalog2 = bootstrapper2.GetCatalog();

TEST_F(CatalogTests, BootstrappingCatalog) {
  EXPECT_EQ(global_catalog->GetDatabaseCount(), 1);
  LOG_INFO("Database name: %s", global_catalog->GetDatabaseWithOid(0)->GetDBName().c_str());
}

TEST_F(CatalogTests, CreatingDatabase) {
	global_catalog2->CreateDatabase("EMP_DB");
  EXPECT_EQ(global_catalog2->GetDatabaseWithName("EMP_DB")->GetDBName(), "EMP_DB");
}

TEST_F(CatalogTests, CreatingTable) {
  auto id_column =
	      catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
	                      "dept_id", true);
  auto name_column =
      catalog::Column(VALUE_TYPE_VARCHAR, 32,
                      "dept_name", false);
  std::unique_ptr<catalog::Schema> table_schema(new catalog::Schema({id_column, name_column}));
  global_catalog->CreateTable("EMP_DB", "department_table", std::move(table_schema));
  EXPECT_EQ(global_catalog->GetDatabaseWithName("EMP_DB")->GetTableWithName("department_table")->GetSchema()->GetColumn(1).GetName(), "dept_name");
}

TEST_F(CatalogTests, DroppingTable) {
  EXPECT_EQ(global_catalog->GetDatabaseWithName("EMP_DB")->GetTableCount(), 1);
  global_catalog->DropTable("EMP_DB", "department_table");
  EXPECT_EQ(global_catalog->GetDatabaseWithName("EMP_DB")->GetTableCount(), 0);

}

}  // End test namespace
}  // End peloton namespace
