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

class Configuration {
 public:
  /**
   * @brief Constructor
   */
  Configuration() {}

 private:
  // The set of hypothetical indexes in the configuration
  std::vector<catalog::IndexCatalogObject> indexes_;

};

}  // namespace brain
}  // namespace peloton
