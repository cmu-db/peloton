/*
 * projection.cpp
 *
 */

#include "../executor/projection.h"

namespace peloton {
namespace executor {

Projection::~Projection(){
  // Delete the expressions
  for(auto e : projection_entries_){
    delete e.second;
  }
}

/**
 * @brief Generate the projected tuple in destination
 *        based on one or two source tuples.
 * @param dest Destination. Must be a physical tuple
 *        as we need to write.
 * @param src1 Source tuple1 passed to expressions.
 * @param src2 Source tuple2 passed to expressions.
 * @return  True if successful. Otherwise false.
 */
bool Projection::Evaluate(storage::Tuple* dest,
                          const AbstractTuple* src1,
                          const AbstractTuple* src2) const {

  for(auto e : projection_entries_){
    auto col_id = e.first;
    auto expression = e.second;
    Value val = expression->Evaluate(src1, src2, nullptr );
    dest->SetValue(col_id, val);
  }
  return true;
}

} /* namespace expression */
} /* namespace peloton */
