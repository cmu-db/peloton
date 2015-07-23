/*
 * projection.h
 *
 * Copyright(c) 2015, CMU
 */

#pragma once

#include <utility>
#include <vector>

#include "backend/common/types.h"
#include "backend/common/value.h"
#include "backend/expression/abstract_expression.h"
#include "backend/storage/tuple.h"


namespace peloton {
namespace executor {

/**
 * TODO: Under Construction.
 */
class ProjectionInfo {

 public:

  ProjectionInfo(ProjectionInfo &) = delete;
  ProjectionInfo operator= (ProjectionInfo &) = delete;
  ProjectionInfo(ProjectionInfo &&) = delete;
  ProjectionInfo operator= (ProjectionInfo &&) = delete;

  /**
   * @brief Generic specification of a projection target:
   *        < DEST_column_id , expression >
   */
  typedef std::pair<oid_t, const expression::AbstractExpression*> Target;

  typedef std::vector<Target> TargetList;

  /**
   * @brief Generic specification of a direct map:
   *        < NEW_col_id , OLD_col_id >
   */
  typedef std::pair<oid_t, oid_t> DirectMap;

  typedef std::vector<DirectMap> DirectMapList;

  ProjectionInfo(TargetList& tl, DirectMapList& dml)
    : target_list_(tl), direct_map_list_(dml){

  }

  ProjectionInfo(TargetList&& tl, DirectMapList&& dml)
    : target_list_(tl), direct_map_list_(dml){

  }

  const TargetList& GetTargetList() const {
    return target_list_;
  }

  const DirectMapList& GetDirectMapList() const {
    return direct_map_list_;
  }

  ~ProjectionInfo();


 private:
  TargetList target_list_;
  DirectMapList direct_map_list_;

};

} /* namespace expression */
} /* namespace peloton */

