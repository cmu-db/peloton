/*
 * project_info.h
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include <utility>
#include <vector>

//#include "backend/common/value.h"
#include "backend/expression/abstract_expression.h"
#include "backend/storage/tuple.h"


namespace peloton {
namespace planner {

/**
 * @brief A class for representing projection information.
 *
 * The information is stored in two parts.
 * 1) A target_list stores non-trivial projections that can be calculated from expressions.
 * 2) A direct_map_list stores projections that is simply reorder of attributes in the input.
 *
 * We separate it in this way for two reasons:
 * i) Postgres does the same thing;
 * ii) It makes it possible to use a more efficient executor to handle pure direct map projections.
 *
 * NB: in case of a constant-valued projection, it is still under the umbrella of \b target_list,
 * though sounds simple enough.
 */
class ProjectInfo {

 public:

  ProjectInfo(ProjectInfo &) = delete;
  ProjectInfo operator= (ProjectInfo &) = delete;
  ProjectInfo(ProjectInfo &&) = delete;
  ProjectInfo operator= (ProjectInfo &&) = delete;

  /**
   * @brief Generic specification of a projection target:
   *        < DEST_column_id , expression >
   */
  typedef std::pair<oid_t, const expression::AbstractExpression*> Target;

  typedef std::vector<Target> TargetList;

  /**
   * @brief Generic specification of a direct map:
   *        < NEW_col_id , <tuple_idx , OLD_col_id>  >
   */
  typedef std::pair<oid_t, std::pair<oid_t, oid_t> > DirectMap;

  typedef std::vector<DirectMap> DirectMapList;

  ProjectInfo(TargetList& tl, DirectMapList& dml)
    : target_list_(tl), direct_map_list_(dml){

  }

  ProjectInfo(TargetList&& tl, DirectMapList&& dml)
    : target_list_(tl), direct_map_list_(dml){

  }

  const TargetList& GetTargetList() const {
    return target_list_;
  }

  const DirectMapList& GetDirectMapList() const {
    return direct_map_list_;
  }

  ~ProjectInfo();


 private:
  TargetList target_list_;
  DirectMapList direct_map_list_;

};

} /* namespace planner */
} /* namespace peloton */

