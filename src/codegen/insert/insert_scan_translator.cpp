//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_group_by_translator.cpp
//
// Identification: src/codegen/insert/insert_scan_translator.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/insert/insert_scan_translator.h"

namespace peloton {
namespace codegen {

InsertScanTranslator::InsertScanTranslator(const planner::InsertPlan &insert_plan,
                                           CompilationContext &context,
                                           Pipeline &pipeline)
    : AbstractInsertTranslator(insert_plan, context, pipeline) {}

void InsertScanTranslator::Produce() const {
  // @todo Implement this.
}

void InsertScanTranslator::Consume(ConsumerContext &context,
                                   RowBatch::Row &row) const {
  (void)context;
  (void)row;
  // @todo Implement this.
}

void InsertScanTranslator::Consume(ConsumerContext &context,
                                   RowBatch &batch) const {
  (void)context;
  (void)batch;
  // @todo Implement this.
}

}  // namespace codegen
}  // namespace peloton
