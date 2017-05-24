//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// insert_scan_translator.h
//
// Identification: src/include/codegen/insert/insert_scan_translator.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/aggregation.h"
#include "codegen/compilation_context.h"
#include "codegen/consumer_context.h"
#include "codegen/insert/abstract_insert_translator.h"
#include "codegen/oa_hash_table.h"
#include "codegen/operator_translator.h"
#include "codegen/updateable_storage.h"
#include "planner/insert_plan.h"

namespace peloton {
namespace codegen {

/**
 * @brief Insert tuples from a scan.
 */
class InsertScanTranslator : public AbstractInsertTranslator {
 public:
  /**
   * @brief Constructor.
   */
  InsertScanTranslator(const planner::InsertPlan &insert_plan,
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
  void Consume(ConsumerContext &context, RowBatch::Row &row) const override;
  void Consume(ConsumerContext &context, RowBatch &batch) const override;

  /**
   * @brief Codegen any cleanup work for this translator.
   */
  void TearDownState() override {}

  /**
   * @brief Get a stringified version of this translator.
   */
  std::string GetName() const override { return "Insert"; }

 private:
  //  std::unique_ptr<type::AbstractPool> pool_;
  //  std::unique_ptr<storage::Tuple> tuple_;
  //  char *tuple_data_;

  mutable llvm::Value *tuple_ptr_;
  mutable llvm::Value *tuple_data_ptr_;
  mutable llvm::Value *pool_ptr_;
};

}  // namespace codegen
}  // namespace peloton
