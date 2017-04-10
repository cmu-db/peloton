//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_group_by_translator.cpp
//
// Identification: src/codegen/hash_group_by_translator.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/insert/insert_translator.h"

#include "codegen/if.h"
#include "codegen/oa_hash_table_proxy.h"
#include "codegen/vectorized_loop.h"

namespace peloton {
namespace codegen {

InsertTranslator::InsertTranslator(const planner::InsertPlan &insert_plan,
                                   CompilationContext &context,
                                   Pipeline &pipeline)
    : OperatorTranslator(context, pipeline), insert_plan_(insert_plan) {

  if (insert_plan.GetChildren().size() == 1) {
    context.Prepare(*insert_plan.GetChild(0), pipeline);
  }

}

void InsertTranslator::Produce() const {
  if (this->insert_plan_.GetChildren().size() == 1) {
    PL_UNIMPLEMENTED("InsertPlan with child");
  } else {

  }
}

void InsertTranslator::Consume(ConsumerContext &context, RowBatch::Row &row) const {
  (void)context;
  (void)row;
}

void InsertTranslator::Consume(ConsumerContext &context, RowBatch &batch) const {
  (void)context;
  (void)batch;
}

}  // namespace codegen
}  // namespace peloton
