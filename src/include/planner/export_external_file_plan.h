//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// export_external_file_plan.h
//
// Identification: src/include/planner/export_external_file_plan.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "concurrency/transaction_context.h"
#include "planner/abstract_plan.h"

namespace peloton {
namespace planner {

class ExportExternalFilePlan : public AbstractPlan {
 public:
  ExportExternalFilePlan(std::string file_name, char delimiter = ',',
                         char quote = '"', char escape = '\"');

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

  PlanNodeType GetPlanNodeType() const override;

  const std::string &GetFileName() const { return file_name_; }

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
  std::vector<const planner::AttributeInfo *> output_attributes_;

  std::string file_name_;

  char delimiter_;
  char quote_;
  char escape_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// Implementation below
///
////////////////////////////////////////////////////////////////////////////////

inline ExportExternalFilePlan::ExportExternalFilePlan(std::string file_name,
                                                      char delimiter,
                                                      char quote, char escape)
    : file_name_(file_name),
      delimiter_(delimiter),
      quote_(quote),
      escape_(escape) {}

inline PlanNodeType ExportExternalFilePlan::GetPlanNodeType() const {
  return PlanNodeType::EXPORT_EXTERNAL_FILE;
}

inline hash_t ExportExternalFilePlan::Hash() const {
  hash_t hash = HashUtil::HashBytes(file_name_.data(), file_name_.length());
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&delimiter_));
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&quote_));
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&escape_));
  return hash;
}

inline bool ExportExternalFilePlan::operator==(const AbstractPlan &rhs) const {
  if (rhs.GetPlanNodeType() != PlanNodeType::EXPORT_EXTERNAL_FILE) return false;
  const auto &other = static_cast<const ExportExternalFilePlan &>(rhs);
  return (
      (StringUtil::Upper(file_name_) == StringUtil::Upper(other.file_name_)) &&
      delimiter_ == other.delimiter_ && quote_ == other.quote_ &&
      escape_ == other.escape_);
}

inline std::unique_ptr<AbstractPlan> ExportExternalFilePlan::Copy() const {
  return std::unique_ptr<AbstractPlan>{
      new ExportExternalFilePlan(file_name_, delimiter_, quote_, escape_)};
}

inline void ExportExternalFilePlan::PerformBinding(
    BindingContext &binding_context) {
  PELOTON_ASSERT(GetChildrenSize() == 1);
  auto &child = *GetChild(0);

  std::vector<oid_t> child_output_cols;
  child.GetOutputColumns(child_output_cols);

  output_attributes_.clear();
  for (const auto &col_id : child_output_cols) {
    output_attributes_.push_back(binding_context.Find(col_id));
  }
}

}  // namespace planner
}  // namespace peloton