//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// order_by_translator.h
//
// Identification: src/include/codegen/operator/order_by_translator.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/compilation_context.h"
#include "codegen/operator/operator_translator.h"
#include "codegen/sorter.h"
#include "codegen/updateable_storage.h"

namespace peloton {

namespace planner {
class OrderByPlan;
}  // namespace planner

namespace codegen {

class OrderByTranslator : public OperatorTranslator {
 public:
  OrderByTranslator(const planner::OrderByPlan &plan,
                    CompilationContext &context, Pipeline &pipeline);

  void InitializeState() override;

  void DefineAuxiliaryFunctions() override;

  void Produce() const override;

  void Consume(ConsumerContext &context, RowBatch::Row &row) const override;

  void TearDownState() override;

  std::string GetName() const override;

 private:
  // Accessor
  const planner::OrderByPlan &GetPlan() const { return plan_; }

  //===--------------------------------------------------------------------===//
  // The call back used when iterating over the results in the sorter instance
  //===--------------------------------------------------------------------===//
  class ProduceResults : public Sorter::VectorizedIterateCallback {
   public:
    ProduceResults(const OrderByTranslator &translator,
                   Vector &selection_vector);
    // The callback function providing the current tuple in the sorter instance
    void ProcessEntries(CodeGen &codegen, llvm::Value *start_index,
                        llvm::Value *end_index,
                        Sorter::SorterAccess &access) const override;

   private:
    // The translator
    const OrderByTranslator &translator_;
    // The selection vector when producing rows
    Vector &selection_vector_;
  };

  //===--------------------------------------------------------------------===//
  // An attribute accessor from a row in the sorter
  //===--------------------------------------------------------------------===//
  class SorterAttributeAccess : public RowBatch::AttributeAccess {
   public:
    SorterAttributeAccess(Sorter::SorterAccess &sorter_access,
                          uint32_t col_index);
    // Access the configured attributes in the provided row
    Value Access(CodeGen &codegen, RowBatch::Row &row) override;

   private:
    // A random access interface to the underlying sorter
    Sorter::SorterAccess &sorter_access_;
    // The column index of the column/attribute we want to access
    uint32_t col_index_;
  };

 private:
  // The plan
  const planner::OrderByPlan &plan_;

  // The child pipeline
  Pipeline child_pipeline_;

  // The ID of our sorter instance in the runtime state
  RuntimeState::StateID sorter_id_;

  // The sorter translator instance
  Sorter sorter_;

  // The comparison function
  llvm::Function *compare_func_;

  struct SortKeyInfo {
    // The sort key
    const planner::AttributeInfo *sort_key;

    // Is the sort key materialized in the tuple?
    bool is_part_of_output;

    // The slot in the materialized tuple to find the sort key column
    uint32_t tuple_slot;
  };

  std::vector<SortKeyInfo> sort_key_info_;
};

}  // namespace codegen
}  // namespace peloton