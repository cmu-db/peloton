/*
 * project_info.cpp
 *
 */

#include "../planner/project_info.h"

namespace peloton {
namespace planner {

/**
 * @brief Mainly release the expression in target list.
 */
ProjectInfo::~ProjectInfo() {
  for (auto target : target_list_) {
    delete target.second;
  }
}

/**
 * @brief Fill in the destination tuple according to projectin info.
 * The destination should be pre-allocated by the caller.
 *
 * @warning Destination should not be the same as any source.
 * @warning If target list and direct map list have overlapping
 * destination columns, the behavior is undefined.
 *
 * @param dest    Destination tuple.
 * @param tuple1  Source tuple 1.
 * @param tuple2  Source tuple 2.
 * @param econtext  ExecutorContext for expression evaluation.
 */
bool ProjectInfo::Evaluate(storage::Tuple* dest, const AbstractTuple* tuple1,
                           const AbstractTuple* tuple2,
                           executor::ExecutorContext* econtext) const {
  // (A) Execute target list
  for (auto target : target_list_) {
    auto col_id = target.first;
    auto expr = target.second;
    auto value = expr->Evaluate(tuple1, tuple2, econtext);

    dest->SetValue(col_id, value);
  }

  // (B) Execute direct map
  for (auto dm : direct_map_list_) {
    auto dest_col_id = dm.first;
    auto tuple_idx = dm.second.first;
    auto src_col_id = dm.second.second;

    Value value = (tuple_idx == 0) ? tuple1->GetValue(src_col_id)
                                   : tuple2->GetValue(src_col_id);

    dest->SetValue(dest_col_id, value);
  }

  return true;
}

} /* namespace planner */
} /* namespace peloton */
