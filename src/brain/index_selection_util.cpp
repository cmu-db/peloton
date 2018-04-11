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

//===--------------------------------------------------------------------===//
// IndexConfiguration
//===--------------------------------------------------------------------===//

IndexConfiguration::IndexConfiguration() {}

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

const std::set<std::shared_ptr<IndexObject>>& IndexConfiguration::GetIndexes() const {
  return indexes_;
}

const std::string IndexConfiguration::ToString() const {
  std::stringstream str_stream;
  for (auto index: indexes_) {
    // str_stream << index->ToString() << " ";
  }
  return str_stream.str();
}

bool IndexConfiguration::operator ==(const IndexConfiguration &config) const {
  auto config_indexes = config.GetIndexes();
  if(config_indexes.size() != indexes_.size()) return false;
  for (uint i = 0; i < indexes_.size(); i++) {
    // if(indexes_[i] != config_indexes[i]) return false;
  }
  return true;
}

//===--------------------------------------------------------------------===//
// IndexObjectPool
//===--------------------------------------------------------------------===//

IndexObjectPool::IndexObjectPool() {}

std::shared_ptr<IndexObject> IndexObjectPool::GetIndexObject(IndexObject &obj) {
  auto ret = map_.find(obj);
  if (ret != map_.end()) {
    return ret->second;
  }
  return nullptr;
}

std::shared_ptr<IndexObject> IndexObjectPool::PutIndexObject(IndexObject &obj) {
  IndexObject *index_copy = new IndexObject();
  *index_copy = obj;
  auto index_s_ptr = std::shared_ptr<IndexObject>(index_copy);
  map_[*index_copy] = index_s_ptr;
  return index_s_ptr;
}

}  // namespace brain
}  // namespace peloton
