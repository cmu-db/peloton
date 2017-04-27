//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// insert_tuples_translator.h
//
// Identification: src/include/codegen/insert/insert_tuples_translator.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/aggregation.h"
#include "codegen/compilation_context.h"
#include "codegen/consumer_context.h"
#include "codegen/oa_hash_table.h"
#include "codegen/operator_translator.h"
#include "codegen/updateable_storage.h"
#include "codegen/insert/abstract_insert_translator.h"
#include "planner/insert_plan.h"

namespace peloton {
namespace codegen {

/**
 * @brief Insert concrete tuples.
 */
class InsertTuplesTranslator : public AbstractInsertTranslator {
 public:

  /**
   * @brief Constructor.
   */
  InsertTuplesTranslator(const planner::InsertPlan &insert_plan,
                         CompilationContext &context, Pipeline &pipeline);
  /**
   * @brief Codegen any initialization work for this operator.
   */
  void InitializeState() override {}

  /**
   * @brief Define any helper functions this translator needs
   */
  void DefineAuxiliaryFunctions() override {}

  /**
   * @brief Codegen produce new tuples.
   */
  void Produce() const override;

  /**
   * @brief The method that consumes tuples from child operators.
   */
  void Consume(ConsumerContext &, RowBatch::Row &) const override {
    PL_UNIMPLEMENTED("Shouldn't be called");
  }

  void Consume(ConsumerContext &, RowBatch &) const override {
    PL_UNIMPLEMENTED("Shouldn't be called");
  }

  /**
   * @brief Codegen any cleanup work for this translator.
   */
  void TearDownState() override {}

  /**
   * @brief Get a stringified version of this translator.
   */
  std::string GetName() const override { return "Insert"; }

 private:
  uint32_t tuples_offset_;
};

}  // namespace codegen
}  // namespace peloton
