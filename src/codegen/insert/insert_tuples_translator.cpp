//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_group_by_translator.cpp
//
// Identification: src/codegen/insert/insert_tuples_translator.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/insert/insert_tuples_translator.h"

namespace peloton {
namespace codegen {

InsertTuplesTranslator::InsertTuplesTranslator(const planner::InsertPlan &insert_plan,
                                               CompilationContext &context,
                                               Pipeline &pipeline)
    : AbstractInsertTranslator(insert_plan, context, pipeline) {

  context.GetRuntimeState().RegisterState("raw_tuples_to_insert", nullptr);

  oid_t num_tuples = insert_plan.GetBulkInsertCount();
  for (decltype(num_tuples) i = 0; i < num_tuples; ++i) {
    const storage::Tuple *tuple = insert_plan.GetTuple(i);
    (void)tuple;
  }
}

void InsertTuplesTranslator::Produce() const {
  // @todo Implement this.
}

}  // namespace codegen
}  // namespace peloton
