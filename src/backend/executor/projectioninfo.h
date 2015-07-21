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
   *        < dest_column_id , expression >
   */
  typedef std::pair<oid_t, expression::AbstractExpression*> Target;

  typedef std::vector<Target> TargetList;

  /**
   * @brief Generic specification of a direct map:
   *        < old_col_id , new_col_id >
   */
  typedef std::pair<oid_t, oid_t> DirectMap;

  typedef std::vector<DirectMap> DirectMapList;

  virtual ~ProjectionInfo();

  virtual const std::vector<Target>& GetProjectionEntries() {
    return projection_entries_;
  }

  virtual void AddProjectionEntry(ProjectionEntry pe) {
    projection_entries_.push_back(pe);
  }

  virtual bool
  Evaluate(storage::Tuple* dest, const AbstractTuple* src1, const AbstractTuple* src2) const;

 protected:


  std::vector<ProjectionEntry> projection_entries_;

};

} /* namespace expression */
} /* namespace peloton */

