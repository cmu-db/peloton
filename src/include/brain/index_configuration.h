//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_configuration.h
//
// Identification: src/include/brain/index_configuration.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "catalog/index_catalog.h"
#include "parser/sql_statement.h"

namespace peloton {
namespace brain {

//===--------------------------------------------------------------------===//
// IndexConfiguration
//===--------------------------------------------------------------------===//

class IndexConfiguration {
 public:
  IndexConfiguration() {}

  // Add indexes of a given IndexConfiguration into this IndexConfiguration.
  void Add(IndexConfiguration &config);

  void AddIndex(std::shared_ptr<catalog::IndexCatalogObject> index);

  const std::vector<std::shared_ptr<catalog::IndexCatalogObject>>
      &GetIndexes() {
    return indexes_;
  }

 private:
  // The set of hypothetical indexes in the IndexConfiguration
  std::vector<std::shared_ptr<catalog::IndexCatalogObject>> indexes_;
};

}  // namespace brain
}  // namespace peloton
