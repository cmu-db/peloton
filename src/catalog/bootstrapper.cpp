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
Catalog* Bootstrapper::bootstrap() {
  if(bootstrapped) {
	  return global_catalog;
  }
  else{
    bootstrapped = true;
    return InitializeGlobalCatalog();
  }
}


// Initialize Global Catalog
Catalog* Bootstrapper::InitializeGlobalCatalog() {
  global_catalog = Catalog::GetInstance().release();
  global_catalog->CreateCatalogDatabase();
  return global_catalog;
}

Catalog* Bootstrapper::GetCatalog(){
	return global_catalog;
}


}
}
