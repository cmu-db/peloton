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

  // Global Singleton
  static Bootstrapper &GetInstance(void);

  // Bootstrap Catalog
  Catalog* bootstrap();

  // Initialize Global Catalog
  Catalog* InitializeGlobalCatalog();

  // Initialize Schemas
  void InitializeCatalogsSchemas();

  Catalog* GetCatalog();

  // TODO: Other Catalogs should be added here

private:
  // The global catalog
  Catalog* global_catalog;
  bool bootstrapped = false;

};
}
}
