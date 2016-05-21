//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// project_info.h
//
// Identification: src/backend/planner/project_info.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <utility>
#include <vector>

#include "backend/expression/abstract_expression.h"
#include "backend/storage/tuple.h"

namespace peloton {

namespace planner {

/**
 * @brief A class for representing projection information.
 *
 * The information is stored in two parts.
 * 1) A target_list stores non-trivial projections that can be calculated from
 *expressions.
 * 2) A direct_map_list stores projections that is simply reorder of attributes
 *in the input.
 *
 * We separate it in this way for two reasons:
 * i) Postgres does the same thing;
 * ii) It makes it possible to use a more efficient executor to handle pure
 * direct map projections.
 *
 * NB: in case of a constant-valued projection, it is still under the umbrella
 * of \b target_list, though it sounds simple enough.
 */
class ProjectInfo {
 public:
  ProjectInfo(ProjectInfo & project_info) = delete;
  ProjectInfo operator=(ProjectInfo &) = delete;
  ProjectInfo(ProjectInfo &&) = delete;
  ProjectInfo operator=(ProjectInfo &&) = delete;

  /* Force explicit move to emphasize the transfer of ownership */
  ProjectInfo(TargetList &tl, DirectMapList &dml) = delete;

  ProjectInfo(TargetList &&tl, DirectMapList &&dml)
      : target_list_(tl), direct_map_list_(dml) {}

  ProjectInfo(const ProjectInfo & project_info) {
    direct_map_list_ = project_info.GetDirectMapList();
    // target_list_ = project_info.GetTargetList();

    for (const Target &target : project_info.GetTargetList()) {
      target_list_.push_back(
        std::pair<oid_t, const expression::AbstractExpression *>(
          target.first, target.second->Copy()));
    }
  }

  const TargetList &GetTargetList() const { return target_list_; }

  const DirectMapList &GetDirectMapList() const { return direct_map_list_; }

  void SetTargetList(const TargetList &target_list) {
    target_list_ = target_list;
  }

  bool isNonTrivial() const { return target_list_.size() > 0; };

  bool Evaluate(storage::Tuple *dest, const AbstractTuple *tuple1,
                const AbstractTuple *tuple2,
                executor::ExecutorContext *econtext) const;

  std::string Debug() const;

  ~ProjectInfo();

  std::unique_ptr<const ProjectInfo> Copy() const {
    std::vector<Target> new_target_list;
    for (const Target &target : target_list_) {
      new_target_list.push_back(
          std::pair<oid_t, const expression::AbstractExpression *>(
              target.first, target.second->Copy()));
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

 private:
  TargetList target_list_;

  DirectMapList direct_map_list_;
};

} /* namespace planner */
} /* namespace peloton */
