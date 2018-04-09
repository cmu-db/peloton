//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// configuration.h
//
// Identification: src/include/brain/configuration.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "catalog/index_catalog.h"

namespace peloton {
namespace brain {

//===--------------------------------------------------------------------===//
// Configuration
//===--------------------------------------------------------------------===//

struct Configuration {
  // Add indexes of a given configuration into this configuration.
  void Add(Configuration &config) {
    auto c_indexes = config.indexes_;
    for (auto index: c_indexes) {
      indexes_.push_back(index);
    }
  }
  // The set of hypothetical indexes in the configuration
  std::vector<std::shared_ptr<catalog::IndexCatalogObject>> indexes_;
};

}  // namespace brain
}  // namespace peloton
