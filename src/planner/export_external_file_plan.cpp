//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// export_external_file_plan.cpp
//
// Identification: src/planner/export_external_file_plan.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/export_external_file_plan.h"

#include "common/macros.h"
#include "util/hash_util.h"
#include "util/string_util.h"

namespace peloton {
namespace planner {

ExportExternalFilePlan::ExportExternalFilePlan(std::string file_name,
                                               char delimiter, char quote,
                                               char escape)
    : file_name_(file_name),
      delimiter_(delimiter),
      quote_(quote),
      escape_(escape) {}

PlanNodeType ExportExternalFilePlan::GetPlanNodeType() const {
  return PlanNodeType::EXPORT_EXTERNAL_FILE;
}

hash_t ExportExternalFilePlan::Hash() const {
  hash_t hash = HashUtil::HashBytes(file_name_.data(), file_name_.length());
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&delimiter_));
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&quote_));
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&escape_));
  return hash;
}

bool ExportExternalFilePlan::operator==(const AbstractPlan &rhs) const {
  if (rhs.GetPlanNodeType() != PlanNodeType::EXPORT_EXTERNAL_FILE) return false;
  const auto &other = static_cast<const ExportExternalFilePlan &>(rhs);
  return (
      (StringUtil::Upper(file_name_) == StringUtil::Upper(other.file_name_)) &&
      delimiter_ == other.delimiter_ && quote_ == other.quote_ &&
      escape_ == other.escape_);
}

std::unique_ptr<AbstractPlan> ExportExternalFilePlan::Copy() const {
  return std::unique_ptr<AbstractPlan>{
      new ExportExternalFilePlan(file_name_, delimiter_, quote_, escape_)};
}

void ExportExternalFilePlan::PerformBinding(BindingContext &binding_context) {
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