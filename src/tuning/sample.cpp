//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sample.cpp
//
// Identification: src/tuning/sample.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "tuning/sample.h"

#include <cmath>
#include <iostream>
#include <sstream>

#include "tuning/sample.h"
#include "common/logger.h"
#include "common/macros.h"

namespace peloton {
namespace tuning {

double Sample::GetDistance(const Sample &other) const {
  double dist = 0;

  // Absolute difference
  size_t column_itr = 0;
  for (auto column : columns_accessed_) {
    auto other_val = other.columns_accessed_[column_itr++];
    dist += std::abs(column - other_val);
  }

  return dist;
}

Sample Sample::GetDifference(const Sample &other) const {
  std::vector<double> columns_accessed;

  size_t column_itr = 0;
  for (auto column : columns_accessed_) {
    auto other_val = other.columns_accessed_[column_itr++];
    columns_accessed.push_back(column - other_val);
  }

  auto diff = Sample(columns_accessed);

  return diff;
}

Sample &Sample::operator*(const double &rhs) {
  size_t column_itr;
  size_t column_count = columns_accessed_.size();

  for (column_itr = 0; column_itr < column_count; column_itr++) {
    this->columns_accessed_[column_itr] *= rhs;
  }

  return *this;
}

Sample &Sample::operator+(const Sample &rhs) {
  size_t column_itr;
  size_t column_count = columns_accessed_.size();

  PELOTON_ASSERT(rhs.columns_accessed_.size() == column_count);

  for (column_itr = 0; column_itr < column_count; column_itr++) {
    auto other_val = rhs.columns_accessed_[column_itr];
    this->columns_accessed_[column_itr] += other_val;
  }

  return *this;
}

std::vector<oid_t> Sample::GetEnabledColumns() const {
  std::vector<oid_t> enabled_columns;

  oid_t column_itr = 0;
  for (auto column : columns_accessed_) {
    if (std::round(column) == 1.0) enabled_columns.push_back(column_itr);
    column_itr++;
  }

  return enabled_columns;
}

const std::string Sample::GetInfo() const {
  std::ostringstream os;

  os << "Sample :: ";

  for (auto column_value : columns_accessed_) os << column_value << " ";

  os << "  ::  " << std::round(weight_);

  return os.str();
}

const std::string Sample::ToString() const {
  std::ostringstream os;
  // This needs to match expected format in BrainUtil::LoadSamplesFile
  // except for the <NAME>
  os << weight_ << " " << columns_accessed_.size() << " ";
  bool first = true;
  for (auto column_value : columns_accessed_) {
    if (first == false) {
      os << " ";
    }
    os << column_value;
    first = false;
  }  // FOR
  return (os.str());
}

bool Sample::operator==(const Sample &other) const {
  if (this->weight_ != other.weight_) return false;
  if (this->sample_type_ != other.sample_type_) return false;

  auto sample_size = columns_accessed_.size();
  if (sample_size != other.columns_accessed_.size()) return (false);
  for (oid_t sample_itr = 0; sample_itr < sample_size; sample_itr++) {
    if (columns_accessed_[sample_itr] != other.columns_accessed_[sample_itr]) {
      return false;
    }
  }

  return true;
}

}  // namespace indextuner
}  // namespace peloton
