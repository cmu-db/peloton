//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bootstrapper.cpp
//
// Identification: src/catalog/bootstrapper.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "catalog/bootstrapper.h"
#include "storage/table_factory.h"


namespace peloton {
namespace catalog {

bool Bootstrapper::bootstrapped = false;
Catalog* Bootstrapper::global_catalog = nullptr;

// Bootstrap Catalog
Catalog* Bootstrapper::bootstrap() {
  if(bootstrapped) {
	  LOG_TRACE("Bootstrapped already!");
	  return global_catalog;
  }
  else{
	  LOG_TRACE("Bootstrapping Catalog...");
    Catalog* catalog = InitializeGlobalCatalog();
    bootstrapped = true;
    return catalog;
  }
  LOG_TRACE("Catalog bootstrapped!");
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
