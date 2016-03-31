//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// catalog_test.cpp
//
// Identification: tests/catalog/catalog_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>

#include "harness.h"

#include "backend/common/logger.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Catalog Tests
//===--------------------------------------------------------------------===//

class CatalogTests : public PelotonTest {};

TEST_F(CatalogTests, BasicTest) {
  EXPECT_EQ(1, 1);
  LOG_INFO("Catalog test");
}

}  // End test namespace
}  // End peloton namespace
