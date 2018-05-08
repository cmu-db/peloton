//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// csv_scan_plan.h
//
// Identification: src/include/planner/csv_scan_plan.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <numeric>

#include "codegen/type/type.h"
#include "planner/abstract_scan_plan.h"
#include "planner/attribute_info.h"

namespace peloton {
namespace planner {

class CSVScanPlan : public AbstractScan {
 public:
  struct ColumnInfo {
    std::string name;
    type::TypeId type;
  };

 public:
  /**
   * Constructs a sequential scan over a CSV file
   *
   * @param file_name The file path
   * @param cols Information of the columns expected in each row of the CSV
   */
  CSVScanPlan(std::string file_name, std::vector<ColumnInfo> &&cols,
              char delimiter = ',', char quote = '"', char escape = '"');

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

  PlanNodeType GetPlanNodeType() const override;

  void GetOutputColumns(std::vector<oid_t> &columns) const override;

  const std::string &GetFileName() const { return file_name_; }

  void GetAttributes(std::vector<const AttributeInfo *> &ais) const override;

  char GetDelimiterChar() const { return delimiter_; }
  char GetQuoteChar() const { return quote_; }
  char GetEscapeChar() const { return escape_; }

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Utilities + Internal
  ///
  //////////////////////////////////////////////////////////////////////////////

  hash_t Hash() const override;

  bool operator==(const AbstractPlan &rhs) const override;

  std::unique_ptr<AbstractPlan> Copy() const override;

  void PerformBinding(BindingContext &binding_context) override;

 private:
  const std::string file_name_;

  char delimiter_;
  char quote_;
  char escape_;

  std::vector<std::unique_ptr<planner::AttributeInfo>> attributes_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// Implementation below
///
////////////////////////////////////////////////////////////////////////////////

inline CSVScanPlan::CSVScanPlan(std::string file_name,
                                std::vector<CSVScanPlan::ColumnInfo> &&cols,
                                char delimiter, char quote, char escape)
    : file_name_(std::move(file_name)),
      delimiter_(delimiter),
      quote_(quote),
      escape_(escape) {
  for (const auto &col : cols) {
    std::unique_ptr<planner::AttributeInfo> attribute{
        new planner::AttributeInfo()};
    attribute->name = col.name;
    attribute->type = codegen::type::Type{col.type, true};
    attributes_.emplace_back(std::move(attribute));
  }
}

inline PlanNodeType CSVScanPlan::GetPlanNodeType() const {
  return PlanNodeType::CSVSCAN;
}

inline std::unique_ptr<AbstractPlan> CSVScanPlan::Copy() const {
  std::vector<CSVScanPlan::ColumnInfo> new_cols;
  for (const auto &attribute : attributes_) {
    new_cols.push_back(CSVScanPlan::ColumnInfo{
        .name = attribute->name, .type = attribute->type.type_id});
  }
  return std::unique_ptr<AbstractPlan>(
      new CSVScanPlan(file_name_, std::move(new_cols)));
}

inline void CSVScanPlan::PerformBinding(BindingContext &binding_context) {
  for (uint32_t i = 0; i < attributes_.size(); i++) {
    binding_context.BindNew(i, attributes_[i].get());
  }
}

inline void CSVScanPlan::GetOutputColumns(std::vector<oid_t> &columns) const {
  columns.clear();
  columns.resize(attributes_.size());
  std::iota(columns.begin(), columns.end(), 0);
}

inline hash_t CSVScanPlan::Hash() const {
  return HashUtil::HashBytes(file_name_.data(), file_name_.length());
}

inline bool CSVScanPlan::operator==(const AbstractPlan &rhs) const {
  if (rhs.GetPlanNodeType() != PlanNodeType::CSVSCAN) return false;
  const auto &other = static_cast<const CSVScanPlan &>(rhs);
  return StringUtil::Upper(file_name_) == StringUtil::Upper(other.file_name_);
}

inline void CSVScanPlan::GetAttributes(
    std::vector<const AttributeInfo *> &ais) const {
  ais.clear();
  for (const auto &ai : attributes_) {
    ais.push_back(ai.get());
  }
}

}  // namespace planner
}  // namespace peloton