//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// project_info.h
//
// Identification: src/include/planner/project_info.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <utility>
#include <vector>

#include "expression/abstract_expression.h"
#include "storage/tuple.h"
#include "util/hash_util.h"

namespace peloton {

namespace expression {
class Parameter;
}

namespace planner {

/**
 * @brief A class for representing projection information.
 *
 * The information is stored in two parts:
 * 1) A target_list stores non-trivial projections that can be calculated from
 *    expressions.
 * 2) A direct_map_list stores projections that is simply reorder of attributes
 *    in the input.
 *
 * We separate it in this way for two reasons:
 * i)  Postgres does the same thing;
 * ii) It makes it possible to use a more efficient executor to handle pure
 *     direct map projections.
 *
 * NB: in case of a constant-valued projection, it is still under the umbrella
 * of \b target_list, though it sounds simple enough.
 */

struct DerivedAttribute {
  AttributeInfo attribute_info;
  const expression::AbstractExpression *expr;

  DerivedAttribute(const expression::AbstractExpression *_expr) : expr(_expr) {}
};

class ProjectInfo {
 public:
  /* Force explicit move to emphasize the transfer of ownership */
  ProjectInfo(TargetList &tl, DirectMapList &dml) = delete;

  ProjectInfo(TargetList &&tl, DirectMapList &&dml)
      : target_list_(tl), direct_map_list_(dml) {}

  ~ProjectInfo();

  void PerformRebinding(
      BindingContext &output_context,
      const std::vector<const BindingContext *> &input_contexts) const;

  void PartitionInputs(std::vector<std::vector<oid_t>> &input) const;

  const TargetList &GetTargetList() const { return target_list_; }

  const DirectMapList &GetDirectMapList() const { return direct_map_list_; }

  bool IsNonTrivial() const { return !target_list_.empty(); };

  bool Evaluate(storage::Tuple *dest, const AbstractTuple *tuple1,
                const AbstractTuple *tuple2,
                executor::ExecutorContext *econtext) const;

  bool Evaluate(AbstractTuple *dest, const AbstractTuple *tuple1,
                const AbstractTuple *tuple2,
                executor::ExecutorContext *econtext) const;

  std::string Debug() const;

  std::unique_ptr<const ProjectInfo> Copy() const {
    std::vector<Target> new_target_list;
    for (const Target &target : target_list_) {
      new_target_list.emplace_back(
          target.first, DerivedAttribute{target.second.expr->Copy()});
    }

    std::vector<DirectMap> new_map_list;
    for (const DirectMap &aMap : direct_map_list_) {
      new_map_list.push_back(std::pair<oid_t, std::pair<oid_t, oid_t>>(
          aMap.first,
          std::pair<oid_t, oid_t>(aMap.second.first, aMap.second.second)));
    }

    return std::unique_ptr<ProjectInfo>(
        new ProjectInfo(std::move(new_target_list), std::move(new_map_list)));
  }

  hash_t Hash() const;

  bool operator==(const ProjectInfo &rhs) const;
  bool operator!=(const ProjectInfo &rhs) const { return !(*this == rhs); }

  virtual void VisitParameters(codegen::QueryParametersMap &map,
      std::vector<peloton::type::Value> &values,
      const std::vector<peloton::type::Value> &values_from_user);

 private:
  bool AreEqual(const planner::DerivedAttribute &A,
                const planner::DerivedAttribute &B) const;

  hash_t Hash(const planner::DerivedAttribute &attribute) const;

 private:
  TargetList target_list_;

  DirectMapList direct_map_list_;

 private:
  DISALLOW_COPY_AND_MOVE(ProjectInfo);
};

}  // namespace planner
}  // namespace peloton
