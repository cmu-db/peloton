//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_manager.h
//
// Identification: src/include/concurrency/transaction_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/delete/delete_translator.h"

namespace peloton {
namespace codegen {

DeleteTranslator::DeleteTranslator(const planner::DeletePlan &delete_plan,
                                   CompilationContext &context,
                                   Pipeline &pipeline)
    : OperatorTranslator(context, pipeline), delete_plan_(delete_plan) {

  // Also create the translator for our child.
  context.Prepare(*delete_plan.GetChild(0), pipeline);
}

void DeleteTranslator::Produce() const {
  auto &compilation_context = this->GetCompilationContext();
  auto &codegen = this->GetCodeGen();
  (void)codegen;

  // child.ForEach(this.Consume)
  compilation_context.Produce(*delete_plan_.GetChild(0));
}

/**
 * @brief The callback function that gets called for every row batch.
 *
 * This simply treats one row at a time, since we delete tuples one at a time.
 *
 * @param context The parent consumers.
 * @param batch A reference of the code-gen'ed "current batch" value.
 */
void DeleteTranslator::Consume(ConsumerContext &context, RowBatch &batch) const {
  batch.Iterate(context.GetCodeGen(), [&](RowBatch::Row &row) {
    this->Consume(context, row);
  });
}

/**
 * @brief Generate code that deals with each tuple.
 *
 * @param context The parent consumers.
 * @param row A reference of the code-gen'ed "current row" value.
 */
void DeleteTranslator::Consume(ConsumerContext &context, RowBatch::Row &row) const {
  // @todo Implement this

  auto &codegen = context.GetCodeGen();

  auto tile_group_id = row.GetTileGroupID();
  auto tuple_id = row.GetTID(codegen);

  codegen.CallPrintf("tile_group_id = %d\n", { tile_group_id });
  codegen.CallPrintf("tuple_id = %d\n", { tuple_id });
}

}  // namespace codegen
}  // namespace peloton
