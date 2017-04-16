//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_factory.h
//
// Identification: src/include/index/index_factory.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "index/index.h"

namespace peloton {
namespace index {

//===--------------------------------------------------------------------===//
// IndexFactory
//===--------------------------------------------------------------------===//

class IndexFactory {
 public:
  // Get an index with required attributes
  static Index *GetIndex(catalog::IndexCatalogObject *index_catalog_object);

 private:
  static std::string GetInfo(catalog::IndexCatalogObject *index_catalog_object,
                             std::string comparatorType);

  //===--------------------------------------------------------------------===//
  // PELOTON::BWTREE
  //===--------------------------------------------------------------------===//

  static Index *GetBwTreeIntsKeyIndex(catalog::IndexCatalogObject *index_catalog_object);

  static Index *GetBwTreeGenericKeyIndex(catalog::IndexCatalogObject *index_catalog_object);

  //===--------------------------------------------------------------------===//
  // PELOTON::SKIPLIST
  //===--------------------------------------------------------------------===//

  static Index *GetSkipListIntsKeyIndex(catalog::IndexCatalogObject *index_catalog_object);

  static Index *GetSkipListGenericKeyIndex(catalog::IndexCatalogObject *index_catalog_object);
};

}  // End index namespace
}  // End peloton namespace
