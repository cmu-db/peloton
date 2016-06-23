//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bootstrap.h
//
// Identification: src/include/catalog/bootstrap.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types.h"
#include "catalog/catalog.h"

namespace peloton {


namespace catalog {

//===--------------------------------------------------------------------===//
// Bootstrap
//===--------------------------------------------------------------------===//

class Bootstrapper {
 public:

  static Catalog* global_catalog;
  static bool bootstrapped;


  // Bootstrap Catalog
  static Catalog* bootstrap();

  // Initialize Global Catalog
  static Catalog* InitializeGlobalCatalog();

  // Initialize Schemas
  static void InitializeCatalogsSchemas();

  static Catalog* GetCatalog();


};
}
}
