//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bootstrap.cpp
//
// Identification: src/catalog/bootstrap.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/bootstrapper.h"
#include "storage/table_factory.h"


namespace peloton {
namespace catalog {


Bootstrapper &Bootstrapper::GetInstance(void) {
  static Bootstrapper bootstrapper;
  return bootstrapper;
}

// Bootstrap Catalog
std::unique_ptr<Catalog> Bootstrapper::bootstrap() { return InitializeGlobalCatalog(); }


// Initialize Global Catalog
std::unique_ptr<Catalog> Bootstrapper::InitializeGlobalCatalog() {
  auto global_catalog = Catalog::GetInstance();
  global_catalog->CreateCatalogDatabase();
  return global_catalog;
}


}
}
