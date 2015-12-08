//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// project_info.cpp
//
// Identification: src/backend/planner/project_info.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/planner/project_info.h"
#include "backend/executor/executor_context.h"

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
 * @brief Evaluate projections from one or two source tuples and
 * put result in destination.
 * The destination should be pre-allocated by the caller.
 *
 * @warning Destination should not be the same as any source.
 *
 * @warning If target list and direct map list have overlapping
 * destination columns, the behavior is undefined.
 *
 * @warning If projected value is not inlined, only a shallow copy is written.
 *
 * @param dest    Destination tuple.
 * @param tuple1  Source tuple 1.
 * @param tuple2  Source tuple 2.
 * @param econtext  ExecutorContext for expression evaluation.
 */
bool ProjectInfo::Evaluate(storage::Tuple *dest, const AbstractTuple *tuple1,
                           const AbstractTuple *tuple2,
                           executor::ExecutorContext *econtext) const {
  // Get varlen pool
  VarlenPool *pool = nullptr;
  if(econtext != nullptr)
    pool = econtext->GetExecutorContextPool();

  // (A) Execute target list
  for (auto target : target_list_) {
    auto col_id = target.first;
    auto expr = target.second;
    auto value = expr->Evaluate(tuple1, tuple2, econtext);

    dest->SetValue(col_id, value, pool);
  }

  // (B) Execute direct map
  for (auto dm : direct_map_list_) {
    auto dest_col_id = dm.first;
    // whether left tuple or right tuple ?
    auto tuple_index = dm.second.first;
    auto src_col_id = dm.second.second;

    Value value = (tuple_index == 0) ? tuple1->GetValue(src_col_id)
                                     : tuple2->GetValue(src_col_id);

    dest->SetValue(dest_col_id, value, pool);
  }

  return true;
}

std::string ProjectInfo::Debug() const {
  std::ostringstream buffer;
  buffer << "Target List: < DEST_column_id , expression >\n";
  for (auto &target : target_list_) {
    buffer << "Dest Col id: " << target.first << std::endl;
    buffer << "Expr: \n" << target.second->Debug(" ");
    buffer << std::endl;
  }
  buffer << "DirectMap List: < NEW_col_id , <tuple_idx , OLD_col_id>  > \n";
  for (auto &dmap : direct_map_list_) {
    buffer << "<" << dmap.first << ", <" << dmap.second.first << ", "
           << dmap.second.second << "> >\n";
  }

  return (buffer.str());
}

} /* namespace planner */
} /* namespace peloton */
