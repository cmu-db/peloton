//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// limit_plan.h
//
// Identification: src/include/planner/limit_plan.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/internal_types.h"
#include "planner/abstract_plan.h"
#include "util/string_util.h"

namespace peloton {
namespace planner {

/**
 * Limit (with Offset) plan node
 */
class LimitPlan : public AbstractPlan {
 public:
  LimitPlan(size_t limit, size_t offset) : limit_(limit), offset_(offset) {}

  /// This class cannot be copied or moved
  DISALLOW_COPY_AND_MOVE(LimitPlan);

  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::LIMIT; }

  const std::string GetInfo() const override;

  void GetOutputColumns(std::vector<oid_t> &columns) const override;

  std::unique_ptr<AbstractPlan> Copy() const override;

  hash_t Hash() const override;

  bool operator==(const AbstractPlan &rhs) const override;

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Member Accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

  size_t GetLimit() const { return limit_; }

  size_t GetOffset() const { return offset_; }

 private:
  // The limit
  const size_t limit_;

  // The offset
  const size_t offset_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// Implementation below
///
////////////////////////////////////////////////////////////////////////////////

inline const std::string LimitPlan::GetInfo() const {
  return StringUtil::Format("Limit[off:%zu,limit:%zu]", offset_, limit_);
}

inline void LimitPlan::GetOutputColumns(std::vector<oid_t> &columns) const {
  PELOTON_ASSERT(GetChildrenSize() == 1 && "Limit expected to have one child");
  GetChild(0)->GetOutputColumns(columns);
}

inline std::unique_ptr<AbstractPlan> LimitPlan::Copy() const {
  return std::unique_ptr<AbstractPlan>(new LimitPlan(limit_, offset_));
}

inline hash_t LimitPlan::Hash() const {
  auto type = GetPlanNodeType();
  hash_t hash = HashUtil::Hash(&type);
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&limit_));
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&offset_));
  return HashUtil::CombineHashes(hash, AbstractPlan::Hash());
}

inline bool LimitPlan::operator==(const AbstractPlan &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType()) {
    return false;
  }
  auto &other = static_cast<const planner::LimitPlan &>(rhs);
  return (limit_ == other.limit_ && offset_ == other.offset_ &&
          AbstractPlan::operator==(rhs));
}

}  // namespace planner
}  // namespace peloton
