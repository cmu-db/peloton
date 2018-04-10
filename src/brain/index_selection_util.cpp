//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// configuration.cpp
//
// Identification: src/brain/configuration.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/index_selection_util.h"
#include "common/logger.h"

namespace peloton {
namespace brain {

IndexConfiguration::IndexConfiguration() {

}

void IndexConfiguration::Add(IndexConfiguration &config) {
  auto indexes = config.GetIndexes();
  for (auto it = indexes.begin(); it != indexes.end(); it++) {
    indexes_.insert(*it);
  }
}

void IndexConfiguration::AddIndexObject(std::shared_ptr<IndexObject> index_info) {
  indexes_.insert(index_info);
}

size_t IndexConfiguration::GetIndexCount() {
  return indexes_.size();
}

std::set<std::shared_ptr<IndexObject>>& IndexConfiguration::GetIndexes() {
  return indexes_;
}

}  // namespace brain
}  // namespace peloton
