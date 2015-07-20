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
class Projection {

 public:

  Projection(Projection &) = delete;
  Projection operator= (Projection &) = delete;
  Projection(Projection &&) = delete;
  Projection operator= (Projection &&) = delete;

  /**
   * @brief Generic specification of a projection entry:
   *        < dest_column_id, expression >
   */
  typedef
      std::pair<oid_t, expression::AbstractExpression*>
  ProjectionEntry;

  virtual ~Projection();

  virtual const std::vector<ProjectionEntry>& GetProjectionEntries() {
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

