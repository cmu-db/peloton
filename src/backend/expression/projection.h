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
namespace expression {

class Projection {

 public:

  /**
   * @brief Generic specification of a projection entry:
   *        < dest_column_id, expression >
   */
  typedef
      std::pair<oid_t, expression::AbstractExpression*>
  ProjectionEntry;

  Projection(){
  }

  Projection(std::vector<ProjectionEntry>& projection_entries)
    : projection_entries_(projection_entries){
  }

  Projection(std::vector<ProjectionEntry>&& projection_entries)
    : projection_entries_(projection_entries){
  }

  virtual ~Projection();

  virtual std::vector<ProjectionEntry>& GetProjectionEntries() {
    return projection_entries_;
  }

  virtual bool
  Evaluate(storage::Tuple* dest, const AbstractTuple* src1, const AbstractTuple* src2) const;



 protected:


  std::vector<ProjectionEntry> projection_entries_;

};

} /* namespace expression */
} /* namespace peloton */

