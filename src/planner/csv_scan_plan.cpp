//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// csv_scan_plan.cpp
//
// Identification: src/planner/csv_scan_plan.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/csv_scan_plan.h"

#include <numeric>

#include "codegen/type/type.h"

namespace peloton {
namespace planner {

CSVScanPlan::CSVScanPlan(std::string file_name,
                         std::vector<CSVScanPlan::ColumnInfo> &&cols,
                         char delimiter, char quote, char escape,
                         std::string null)
    : file_name_(std::move(file_name)),
      delimiter_(delimiter),
      quote_(quote),
      escape_(escape),
      null_(null) {
  attributes_.resize(cols.size());
  for (uint32_t i = 0; i < cols.size(); i++) {
    const auto &col_info = cols[i];
    attributes_[i].type = codegen::type::Type{col_info.type, true};
    attributes_[i].attribute_id = i;
    attributes_[i].name = col_info.name;
  }
}

PlanNodeType CSVScanPlan::GetPlanNodeType() const {
  return PlanNodeType::CSVSCAN;
}

std::unique_ptr<AbstractPlan> CSVScanPlan::Copy() const {
  std::vector<CSVScanPlan::ColumnInfo> new_cols;
  for (const auto &attribute : attributes_) {
    new_cols.push_back(CSVScanPlan::ColumnInfo{.name = attribute.name,
                                               .type = attribute.type.type_id});
  }
  return std::unique_ptr<AbstractPlan>(
      new CSVScanPlan(file_name_, std::move(new_cols)));
}

void CSVScanPlan::PerformBinding(BindingContext &binding_context) {
  for (uint32_t i = 0; i < attributes_.size(); i++) {
    binding_context.BindNew(i, &attributes_[i]);
  }
}

void CSVScanPlan::GetOutputColumns(std::vector<oid_t> &columns) const {
  columns.clear();
  columns.resize(attributes_.size());
  std::iota(columns.begin(), columns.end(), 0);
}

hash_t CSVScanPlan::Hash() const {
  hash_t hash = HashUtil::HashBytes(file_name_.data(), file_name_.length());
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&delimiter_));
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&quote_));
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&escape_));
  hash = HashUtil::CombineHashes(
      hash, HashUtil::HashBytes(null_.c_str(), null_.length()));
  return hash;
}

bool CSVScanPlan::operator==(const AbstractPlan &rhs) const {
  if (rhs.GetPlanNodeType() != PlanNodeType::CSVSCAN) return false;
  const auto &other = static_cast<const CSVScanPlan &>(rhs);
  return (
      (StringUtil::Upper(file_name_) == StringUtil::Upper(other.file_name_)) &&
      delimiter_ == other.delimiter_ && quote_ == other.quote_ &&
      escape_ == other.escape_);
}

void CSVScanPlan::GetAttributes(std::vector<const AttributeInfo *> &ais) const {
  ais.clear();
  for (const auto &ai : attributes_) {
    ais.push_back(&ai);
  }
}

}  // namespace planner
}  // namespace peloton