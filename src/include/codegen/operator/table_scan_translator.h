//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_scan_translator.h
//
// Identification: src/include/codegen/operator/table_scan_translator.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/compilation_context.h"
#include "codegen/consumer_context.h"
#include "codegen/operator/operator_translator.h"
#include "codegen/scan_callback.h"
#include "codegen/table.h"

namespace peloton {

namespace planner {
class SeqScanPlan;
}  // namespace planner

namespace storage {
class DataTable;
}  // namespace storage

namespace codegen {

//===----------------------------------------------------------------------===//
// A translator for table scans
//===----------------------------------------------------------------------===//
class TableScanTranslator : public OperatorTranslator {
 public:
  // Constructor
  TableScanTranslator(const planner::SeqScanPlan &scan,
                      CompilationContext &context, Pipeline &pipeline);

  void InitializeState() override {}

  // Table scans don't rely on any auxiliary functions
  void DefineAuxiliaryFunctions() override {}

  // The method that produces new tuples
  void Produce() const override;

  // Scans are leaves in the query plan and, hence, do not consume tuples
  void Consume(ConsumerContext &, RowBatch &) const override {}
  void Consume(ConsumerContext &, RowBatch::Row &) const override {}

  // Similar to InitializeState(), table scans don't have any state
  void TearDownState() override {}

  // Get a stringified version of this translator
  std::string GetName() const override;

 private:
  //===--------------------------------------------------------------------===//
  // An attribute accessor that uses the backing tile group to access columns
  //===--------------------------------------------------------------------===//
  class AttributeAccess : public RowBatch::AttributeAccess {
   public:
    // Constructor
    AttributeAccess(const TileGroup::TileGroupAccess &access,
                    const planner::AttributeInfo *ai);

    // Access an attribute in the given row
    codegen::Value Access(CodeGen &codegen, RowBatch::Row &row) override;

    const planner::AttributeInfo *GetAttributeRef() const { return ai_; }

   private:
    // The accessor we use to load column values
    const TileGroup::TileGroupAccess &tile_group_access_;
    // The attribute we will access
    const planner::AttributeInfo *ai_;
  };

  //===--------------------------------------------------------------------===//
  // The class responsible for generating vectorized scan code over tile groups
  //===--------------------------------------------------------------------===//
  class ScanConsumer : public codegen::ScanCallback {
   public:
    // Constructor
    ScanConsumer(const TableScanTranslator &translator,
                 Vector &selection_vector);

    // The callback when starting iteration over a new tile group
    void TileGroupStart(CodeGen &, llvm::Value *tile_group_id,
                        llvm::Value *tile_group_ptr) override {
      tile_group_id_ = tile_group_id;
      tile_group_ptr_ = tile_group_ptr;
    }

    // The code that forms the body of the scan loop
    void ProcessTuples(CodeGen &codegen, llvm::Value *tid_start,
                       llvm::Value *tid_end,
                       TileGroup::TileGroupAccess &tile_group_access) override;

    // The callback when finishing iteration over a tile group
    void TileGroupFinish(CodeGen &, llvm::Value *) override {}

   private:
    // Get the predicate, if one exists
    const expression::AbstractExpression *GetPredicate() const;

    void SetupRowBatch(RowBatch &batch,
                       TileGroup::TileGroupAccess &tile_group_access,
                       std::vector<AttributeAccess> &access) const;

    void FilterRowsByVisibility(CodeGen &codegen, llvm::Value *tid_start,
                                llvm::Value *tid_end,
                                Vector &selection_vector) const;

    // Filter all the rows whose TIDs are in the range [tid_start, tid_end] and
    // store their TIDs in the output TID selection vector
    void FilterRowsByPredicate(CodeGen &codegen,
                               const TileGroup::TileGroupAccess &access,
                               llvm::Value *tid_start, llvm::Value *tid_end,
                               Vector &selection_vector) const;

    llvm::Value *SIMDFilterRows(RowBatch &batch,
                                const TileGroup::TileGroupAccess &access) const;

   private:
    // The translator instance the consumer is generating code for
    const TableScanTranslator &translator_;

    // The selection vector used for vectorized scans
    Vector &selection_vector_;

    // The current tile group id we're scanning over
    llvm::Value *tile_group_id_;

    // The current tile group we're scanning over
    llvm::Value *tile_group_ptr_;
  };

  // Plan accessor
  const planner::SeqScanPlan &GetScanPlan() const { return scan_; }

  // Table accessor
  const storage::DataTable &GetTable() const;

 private:
  // The scan
  const planner::SeqScanPlan &scan_;

  // The code-generating table instance
  codegen::Table table_;
};

}  // namespace codegen
}  // namespace peloton