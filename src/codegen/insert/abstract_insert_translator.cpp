//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_insert_translator.cpp
//
// Identification: src/codegen/insert/abstract_insert_translator.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/insert/abstract_insert_translator.h"

#include "codegen/if.h"
#include "codegen/oa_hash_table_proxy.h"
#include "codegen/vectorized_loop.h"

namespace peloton {
namespace codegen {

AbstractInsertTranslator::AbstractInsertTranslator(
    const planner::InsertPlan &insert_plan,
    CompilationContext &context,
    Pipeline &pipeline)
    : OperatorTranslator(context, pipeline), insert_plan_(insert_plan) {}

}  // namespace codegen
}  // namespace peloton
