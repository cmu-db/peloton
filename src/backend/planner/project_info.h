//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// project_info.h
//
// Identification: src/backend/planner/project_info.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
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
  ProjectInfo(ProjectInfo &) = delete;
  ProjectInfo operator=(ProjectInfo &) = delete;
  ProjectInfo(ProjectInfo &&) = delete;
  ProjectInfo operator=(ProjectInfo &&) = delete;

  /**
   * @brief Generic specification of a projection target:
   *        < DEST_column_id , expression >
   */
  typedef std::pair<oid_t, const expression::AbstractExpression* > Target;

  typedef std::vector<Target> TargetList;

  /**
   * @brief Generic specification of a direct map:
   *        < NEW_col_id , <tuple_index (left or right tuple), OLD_col_id>    >
   */
  typedef std::pair<oid_t, std::pair<oid_t, oid_t>> DirectMap;

  typedef std::vector<DirectMap> DirectMapList;

  /* Force explicit move to emphasize the transfer of ownership */
  ProjectInfo(TargetList &tl, DirectMapList &dml) = delete ;

  ProjectInfo(TargetList &&tl, DirectMapList &&dml)
      : target_list_(tl), direct_map_list_(dml) {}

  const TargetList &GetTargetList() const { return target_list_; }

  const DirectMapList &GetDirectMapList() const { return direct_map_list_; }

  bool isNonTrivial() const { return target_list_.size() > 0; };

  bool Evaluate(storage::Tuple *dest, const AbstractTuple *tuple1,
                const AbstractTuple *tuple2,
                executor::ExecutorContext *econtext) const;

  Value Evaluate(oid_t column_id,
                 const AbstractTuple *tuple1,
                 const AbstractTuple *tuple2,
                 executor::ExecutorContext *econtext) const;

  std::string Debug() const;

  ~ProjectInfo();

 private:

  TargetList target_list_;

  DirectMapList direct_map_list_;
};

} /* namespace planner */
} /* namespace peloton */
