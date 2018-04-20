//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_selection_util.cpp
//
// Identification: src/brain/index_selection_util.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/index_selection_util.h"
#include "common/logger.h"

namespace peloton {
namespace brain {

//===--------------------------------------------------------------------===//
// IndexObject
//===--------------------------------------------------------------------===//

const std::string IndexObject::ToString() const {
  std::stringstream str_stream;
  str_stream << "Database: " << db_oid << "\n";
  str_stream << "Table: " << table_oid << "\n";
  str_stream << "Columns: ";
  for (auto col : column_oids) {
    str_stream << col << ", ";
  }
  str_stream << "\n";
  return str_stream.str();
}

bool IndexObject::operator==(const IndexObject &obj) const {
  if (db_oid == obj.db_oid && table_oid == obj.table_oid &&
      column_oids == obj.column_oids) {
    return true;
  }
  return false;
}

bool IndexObject::IsCompatible(std::shared_ptr<IndexObject> index) const {
  return (db_oid == index->db_oid) && (table_oid == index->table_oid);
}

IndexObject IndexObject::Merge(std::shared_ptr<IndexObject> index) {
  IndexObject result;
  result.db_oid = db_oid;
  result.table_oid = table_oid;
  result.column_oids = column_oids;
  for (auto column : index->column_oids) {
    result.column_oids.insert(column);
  }
  return result;
}

//===--------------------------------------------------------------------===//
// IndexConfiguration
//===--------------------------------------------------------------------===//

void IndexConfiguration::Merge(IndexConfiguration &config) {
  auto indexes = config.GetIndexes();
  for (auto it = indexes.begin(); it != indexes.end(); it++) {
    indexes_.insert(*it);
  }
}

void IndexConfiguration::Set(IndexConfiguration &config) {
  indexes_.clear();
  auto indexes = config.GetIndexes();
  for (auto it = indexes.begin(); it != indexes.end(); it++) {
    indexes_.insert(*it);
  }
}

void IndexConfiguration::RemoveIndexObject(
    std::shared_ptr<IndexObject> index_info) {
  indexes_.erase(index_info);
}

void IndexConfiguration::AddIndexObject(
    std::shared_ptr<IndexObject> index_info) {
  indexes_.insert(index_info);
}

size_t IndexConfiguration::GetIndexCount() const { return indexes_.size(); }

bool IndexConfiguration::IsEmpty() const { return indexes_.size() == 0; }

const std::set<std::shared_ptr<IndexObject>> &IndexConfiguration::GetIndexes()
    const {
  return indexes_;
}

const std::string IndexConfiguration::ToString() const {
  std::stringstream str_stream;
  str_stream << "Num of indexes: " << GetIndexCount() << "\n";
  for (auto index : indexes_) {
    str_stream << index->ToString() << " ";
  }
  return str_stream.str();
}

bool IndexConfiguration::operator==(const IndexConfiguration &config) const {
  auto config_indexes = config.GetIndexes();
  return indexes_ == config_indexes;
}

IndexConfiguration IndexConfiguration::operator-(
    const IndexConfiguration &config) {
  auto config_indexes = config.GetIndexes();

  std::set<std::shared_ptr<IndexObject>> result;
  std::set_difference(indexes_.begin(), indexes_.end(), config_indexes.begin(),
                      config_indexes.end(),
                      std::inserter(result, result.end()));
  return IndexConfiguration(result);
}

void IndexConfiguration::Clear() { indexes_.clear(); }

//===--------------------------------------------------------------------===//
// IndexObjectPool
//===--------------------------------------------------------------------===//

std::shared_ptr<IndexObject> IndexObjectPool::GetIndexObject(IndexObject &obj) {
  auto ret = map_.find(obj);
  if (ret != map_.end()) {
    return ret->second;
  }
  return nullptr;
}

std::shared_ptr<IndexObject> IndexObjectPool::PutIndexObject(IndexObject &obj) {
  auto index_s_ptr = GetIndexObject(obj);
  if (index_s_ptr != nullptr) return index_s_ptr;
  IndexObject *index_copy = new IndexObject();
  *index_copy = obj;
  index_s_ptr = std::shared_ptr<IndexObject>(index_copy);
  map_[*index_copy] = index_s_ptr;
  return index_s_ptr;
}

}  // namespace brain
}  // namespace peloton
