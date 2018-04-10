//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_configuration.cpp
//
// Identification: src/brain/index_configuration.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/index_configuration.h"
#include "common/logger.h"

namespace peloton {
namespace brain {

void IndexConfiguration::Add(IndexConfiguration &config) {
  auto c_indexes = config.GetIndexes();
  for (auto index : c_indexes) {
    indexes_.push_back(index);
  }
}

void IndexConfiguration::AddIndex(
    std::shared_ptr<catalog::IndexCatalogObject> index) {
  indexes_.push_back(index);
}

}  // namespace brain
}  // namespace peloton
