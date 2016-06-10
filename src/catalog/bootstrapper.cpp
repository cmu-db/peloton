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
Catalog* Bootstrapper::bootstrap() { return InitializeGlobalCatalog(); }


// Initialize Global Catalog
Catalog* Bootstrapper::InitializeGlobalCatalog() {
  auto &global_catalog = Catalog::GetInstance();

  return &global_catalog;
}


}
}
