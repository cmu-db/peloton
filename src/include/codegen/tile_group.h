//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tile_group.h
//
// Identification: src/include/codegen/tile_group.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/value.h"

namespace peloton {

namespace catalog {
class Schema;
}  // namespace catalog

namespace codegen {

class ScanCallback;

//===----------------------------------------------------------------------===//
// Like codegen::Table, this class is the main entry point for code generation
// for tile groups. An instance of this class should exist for each instance of
// codegen::Table (in fact, this should be a member variable).  The schema of
// the tile group is that of the whole table. We abstract away the fact that
// each _individual_ tile group by have a different layout.
//===----------------------------------------------------------------------===//
class TileGroup {
 public:
  // Constructor
  TileGroup(const catalog::Schema &schema);

  // Generate code that performs a sequential scan over the provided tile group
  void GenerateTidScan(CodeGen &codegen, llvm::Value *tile_group_ptr,
                       llvm::Value *column_layouts, uint32_t batch_size,
                       ScanCallback &consumer) const;

  llvm::Value *GetNumTuples(CodeGen &codegen, llvm::Value *tile_group) const;

  llvm::Value *GetTileGroupId(CodeGen &codegen, llvm::Value *tile_group) const;

 private:
  // A struct to capture enough information to perform strided accesses
  struct ColumnLayout {
    uint32_t col_id;
    llvm::Value *col_start_ptr;
    llvm::Value *col_stride;
    llvm::Value *is_columnar;
    // this is the ptr to element array
    llvm::Value *element_array;
    llvm::Value *is_dict_encoded;
  };

  /*
  //===--------------------------------------------------------------------===//
  // A convenience class to access to a column
  //===--------------------------------------------------------------------===//
  class ColumnAccess {
   public:
    ColumnAccess(CodeGen &codegen, llvm::Value *column_layout, oid_t column_id);

    // Load a vector of columns starting at the provided TID
    llvm::Value *LoadVector(llvm::Value *tid, uint32_t vector_size);

    // Get the pointer to the first value of the column
    llvm::Value *GetColumnPtr();

    // Get the column's stride in the tile group
    llvm::Value *GetStride();

    // Is the column stored in columnar form?
    llvm::Value *IsColumnar();

   private:
    CodeGen &codegen_;
    llvm::Value *column_layout_;

    oid_t column_id_;
    llvm::Value *col_ptr_;
    llvm::Value *col_stride_;
    llvm::Value *is_columnar_;
  };
  */

  std::vector<TileGroup::ColumnLayout> GetColumnLayouts(
      CodeGen &codegen, llvm::Value *tile_group_ptr,
      llvm::Value *column_layout_infos) const;

  // Access a given column for the row with the given tid
  codegen::Value LoadColumn(CodeGen &codegen, llvm::Value *tid,
                            const TileGroup::ColumnLayout &layout) const;

 public:
  //===--------------------------------------------------------------------===//
  // A convenience class that allows generic access (i.e., either row-oriented
  // or column-oriented) to the tile group.
  //===--------------------------------------------------------------------===//
  class TileGroupAccess {
   public:
    // Constructor
    TileGroupAccess(const TileGroup &tile_group,
                    const std::vector<ColumnLayout> &tile_group_layout);

    //===------------------------------------------------------------------===//
    // A row in this tile group
    //===------------------------------------------------------------------===//
    class Row {
     public:
      // Constructor
      Row(const TileGroup &tile_group,
          const std::vector<ColumnLayout> &tile_group_layout, llvm::Value *tid);

      // Load the column at the given index
      codegen::Value LoadColumn(CodeGen &codegen, uint32_t col_idx) const;

      inline llvm::Value *GetTID() const { return tid_; }

     private:
      // The tile group this row belongs to
      const TileGroup &tile_group_;
      // The layout of the tile group
      const std::vector<ColumnLayout> &layout_;
      // The tid of the row
      llvm::Value *tid_;
    };

    // Load a specific row from the batch
    Row GetRow(llvm::Value *tid) const;

    //===------------------------------------------------------------------===//
    // ACCESSORS
    //===------------------------------------------------------------------===//

    inline const TileGroup &GetTileGroup() const { return tile_group_; }

    inline const ColumnLayout &GetLayout(uint32_t col_idx) const {
      return layout_[col_idx];
    }

   private:
    // The tile group
    const TileGroup &tile_group_;
    // The layout of all columns in the tile group
    const std::vector<ColumnLayout> &layout_;
  };

 private:
  // The schema for all tile groups. Each tile group may have a different
  // configuration of tiles (that each have a different schema), but at this
  // level, we do not care.
  const catalog::Schema &schema_;
};

}  // namespace codegen
}  // namespace peloton
